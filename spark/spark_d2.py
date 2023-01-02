from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.functions import expr
import pyspark.sql.functions as F
import pyspark.sql.types as T

conf = SparkConf().setAppName("conf pro spark").setMaster("local")
sc=SparkContext.getOrCreate(conf)

spark = SparkSession.builder.appName("Python airflow d2").config("spark.some.config.option","some-value").getOrCreate()
df1 = spark.read.option('header','true').option('inferSchema','true').csv('/home/momo/Bureau/nifi_transformer/resultats-par-niveau-cirlg-t1-france-entiere_Résultats par niveau CirLG T1 F.csv')

df1.printSchema()

df2 = spark.read.option('header','true').option('inferSchema','true').csv('/home/momo/Bureau/nifi_transformer/resultats-par-niveau-cirlg-t2-france-entiere_Résultats par niveau CirLG T2 F.csv')

df2.printSchema()

"""----------------------------------- avec kafka ---------------------------------------------
df = spark.read.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "ValidatedRecords").option("startingOffsets", "earliest").option("includeHeaders", "true").load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
df.printSchema()

data.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "ValidatedRecords") \
        .save()
"""
#--------------------------------------------------------------------------------------
df1v = df1.withColumnRenamed('Code du département','Code_du_departement1')\
          .withColumnRenamed('Code de la circonscription','Code_de_la_circonscription1')\
          .withColumnRenamed('Libellé de la circonscription','Libelle_de_la_circonscription1')\
          .withColumnRenamed('Etat saisie','Etat_saisie1')\
          .withColumnRenamed('Libellé du département','Libelle_du_departement1')\
          .withColumnRenamed('Abstentions','Abstentions1')\
         .withColumnRenamed('% Abs/Ins','AbsIns1')\
            .withColumnRenamed('Votants','Votants1')\
                .withColumnRenamed('% Vot/Ins','VotIns1')\
                    .withColumnRenamed('Blancs','Blancs1')\
                        .withColumnRenamed('% Blancs/Ins','BlancsIns1')\
                            .withColumnRenamed('% Blancs/Vot','BlancsVot1')\
                                .withColumnRenamed('Nuls','Nuls1')\
                                    .withColumnRenamed('% Nuls/Ins','NulsIns1')\
                                        .withColumnRenamed('% Nuls/Vot','NulsVot1')\
                                            .withColumnRenamed('Exprimés','Exprimes1')\
                                                .withColumnRenamed("Voix23","Voix1")\
                                                .withColumnRenamed("Voix30","Voix2")\
                                                .withColumnRenamed('% Exp/Ins','ExpIns1')\
                                                .withColumnRenamed('% Exp/Vot','ExpVot1')
                                                    

#-------------------------------------------------------------------------------------------
df2v = df2.withColumnRenamed('% Voix/Exp32','pourc_voix_exp2')\
            .withColumnRenamed('% Voix/Ins31','pourc_voix_ins2')\
            .withColumnRenamed('% Voix/Exp25','pourc_voix_exp1')\
            .withColumnRenamed('% Voix/Ins24','pourc_voix_ins1')\
            .withColumnRenamed('Sexe20','sexe1')\
            .withColumnRenamed('Sexe27','sexe2')\
            .withColumnRenamed('Voix23','Vo')\
            .withColumnRenamed('Voix30','Vo1')\
            .withColumnRenamed('Nom21','nom1')\
            .withColumnRenamed('Prénom22','prenom1')\
            .withColumnRenamed('Nom28','nom2')\
            .withColumnRenamed('Prénom29','prenom2')\
            .withColumnRenamed('N°Panneau19','panneau1')\
            .withColumnRenamed('N°Panneau26','panneau2')\
            .withColumnRenamed('Code du département','Code_du_departement2')\
            .withColumnRenamed('Code de la circonscription','Code_de_la_circonscription2')\
            .withColumnRenamed('Libellé du département','Libelle_du_departement2')\
            .withColumnRenamed('Libellé de la circonscription','Libelle_de_la_circonscription2')\
            .withColumnRenamed('Etat saisie','Etat_saisie2')\
            .withColumnRenamed('Abstentions','Abstentions2')\
            .withColumnRenamed('Inscrits','Inscrits1')\
            .withColumnRenamed('Inscrits','Inscrits2')\
                .withColumnRenamed('% Abs/Ins','AbsIns2')\
                .withColumnRenamed('Votants','Votants2')\
                .withColumnRenamed('% Vot/Ins','VotIns2')\
                    .withColumnRenamed('Blancs','Blancs2')\
                        .withColumnRenamed('% Blancs/Ins','BlancsIns2')\
                            .withColumnRenamed('% Blancs/Vot','BlancsVot2')\
                                .withColumnRenamed('Nuls','Nuls2')\
                                    .withColumnRenamed('% Nuls/Ins','NulsIns2')\
                                        .withColumnRenamed('% Nuls/Vot','NulsVot2')\
                                            .withColumnRenamed('Exprimés','Exprimes2')\
                                                .withColumnRenamed('% Exp/Ins','ExpIns2')\
                                                    .withColumnRenamed('% Exp/Vot','ExpVot2')
#----------------------------------------------------------------------------------------------------


dfone = df1v.join(df2v,df1v.Code_de_la_circonscription1 == df2v.Code_de_la_circonscription2,'left')

dfone.printSchema()


#-------------------------------------------------------------------------------------------------

dfone_group=dfone.groupBy("Code_de_la_circonscription1","Code_de_la_circonscription2","Code_du_departement1","Code_du_departement2","nom1","nom2").agg(F.sum("Inscrits1"),F.sum("Vo").alias("sum voix 1"),F.avg("pourc_voix_ins1"))

dfone_group.show()

#--------------------------------------------------------------------------------------------------
dfone_group=dfone.groupBy("Code_de_la_circonscription1").count()
dfone_group.show()

#-------------------------------------------------------------------------------------------------
dfnaone=dfone.na.drop()
dfnaone.count()

dist_data=dfone.distinct()
print("************Distinct count**************** :"+str(dist_data.count()))

drop_dup_data=dfone.dropDuplicates()
print("****************Distinct count drop******************* :"+str(drop_dup_data.count()))

#--------------------------------------------------------------------------------------------------
from pyspark.sql.functions import col,isnan,when,count

dfone.select([count(when(isnan(c) | col(c).isNull(),c)).alias(c) for c in dfone.columns]).show()

df_fil=dfone.select([count(when(col(c).contains('None') | \
    col(c).contains('NULL') | \
    (col(c) == '') | \
    col(c).isNull() | \
    isnan(c),c)).alias(c)
    for c in dfone.columns])

df_fil.show()

