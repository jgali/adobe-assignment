"""
EMR - Spark Script
======================================================================================================================
product_rev_bydomain.py
Description: product or keyword revenue by domain
Job Command :
spark-submit --master yarn --deploy-mode cluster --conf spark.yarn.executor.extraClassPath=./ \
--driver-memory 5G --executor-memory 5G \
s3://jgali-adobe-assignment/code/product_rev_by_domain.py \
-src_path s3://jgali-adobe-assignment/input/sample.tsv \
-tgt_path s3://jgali-adobe-assignment/output/prod_rev_domain/

"""
try:
    import logging
    import argparse
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
    from pyspark.sql import SQLContext, HiveContext
    from pyspark.sql.functions import col, split
except ImportError as e:
    print("Error importing Spark Modules", e)
    sys.exit(1)
logger = logging.getLogger("product_rev_bydomain")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())
class product_revenue_by_domain(object):
    def __init__(self, **argsv):
        logger.info("Starting Spark Session")
        self.spark= SparkSession.builder\
                                .appName("product_rev_by_domain")\
                                .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")\
                                .enableHiveSupport()\
                                .getOrCreate()
        self.src_path = argsv['src_path'].lower()
        self.tgt_path = argsv['tgt_path'].lower()
        logger.info("source path for input file                       : {}".format(self.src_path))
        logger.info("Target path for outputfile                       : {}".format(self.tgt_path))
    def load_data(self):
        logger.info("********* Input parameters for spark data processing and loading data *********")
        logger.info("source path for input file                       : {}".format(self.src_path))
        logger.info("Target path for outputfile                       : {}".format(self.tgt_path))
        logger.info("*************************************")
        try:
            logger.info("INFO : Loading Data...")
            self.spark.read.csv(self.src_path, sep=r'\t', header=True).registerTempTable("src")
            self.spark.sql("select  ip, SPLIT(event_list,',') as event_list, referrer, SPLIT(product_list,',') as product_list, SPLIT(page_url,'.')[1] as page_url from src").registerTempTable("s1")
            self.spark.sql("select ip, event_list, referrer, products_list from s1 lateral view outer explode(product_list) as products_list").registerTempTable("s2")
            self.spark.sql("select *, SPLIT(products_list,';')[3] as revenue from s2 where array_contains(event_list,'1')").registerTempTable("s3")
            self.spark.sql("""select
                            ip,
                            SPLIT(referrer,'\\\\.')[1] as domain,
                            referrer
                            from src where SPLIT(page_url,'\\\\.')[1] != SPLIT(referrer,'\\\\.')[1]""").registerTempTable("srch")
            self.spark.sql("""select
                            ip,
                            upper(domain) as domain,
                            (case when upper(domain) = 'GOOGLE' THEN upper(replace(SPLIT(substr(referrer,instr(referrer,'&q=') + 3),'&')[0], '+',' '))
                            when upper(domain) = 'BING' THEN upper(replace(SPLIT(substr(referrer,instr(referrer,'?q=') + 3),'&')[0], '+',' '))
                            when upper(domain) = 'YAHOO' THEN upper(replace(SPLIT(substr(referrer,instr(referrer,'?p=') + 3),'&')[0], '+',' '))
                            end) as search_keyword
                            from srch
                            """).registerTempTable("srch1")
            self.spark.sql("select domain, search_keyword, sum(revenue) as revenue from s3 inner join srch1 on s3.ip = srch1.ip group by domain, search_keyword order by revenue desc").coalesce(1).write.option("header","true").option("sep","\t").mode("overwrite").csv(path=self.tgt_path)
            logger.info(" Loaded data to :" + self.tgt_path)
            logger.info("INFO: JOB SUCCESSFUL")
        except Exception as ex:
            logger.info("INFO : JOB FAILED with exception")
            raise ex
    def main(self):
        self.load_data()
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="input of arguments")
    parser.add_argument('-src_path', '--src_path', help="src_path")
    parser.add_argument('-tgt_path', '--tgt_path', help="tgt_path")
    argsv = vars(parser.parse_args())
    process_prod_rev_domain = product_revenue_by_domain(**argsv)
    process_prod_rev_domain.main()
