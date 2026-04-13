import duckdb
db_path = r'D:\实战项目\Dataanalysis\Duckdb\project.duckdb'
con = duckdb.connect(database=db_path)

csv_path = r'D:\实战项目\Dataanalysis\archive\*.csv'


sample_query = f"""
    CREATE TABLE sample_events AS 
    SELECT * FROM read_csv_auto('{csv_path}')
    USING SAMPLE 1% (bernoulli);
"""
con.execute(sample_query)
con.close()