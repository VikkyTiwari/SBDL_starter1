import sys

from lib import Utils
from lib.logger import Log4j

if __name__ == '__main__':
    #print("sys.argv: ", sys.argv)
    #print("len(sys.argv): ", len(sys.argv))
    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    #arg0 = sys.argv[0]
    #print("arg0: " + arg0)
    job_run_env = sys.argv[1].upper()
    #print("job_run_env: " + job_run_env)
    load_date = sys.argv[2]
    #print("load_date: " + load_date)

    spark = Utils.get_spark_session(job_run_env)
    logger = Log4j(spark)

    logger.info("Finished creating Spark Session")
