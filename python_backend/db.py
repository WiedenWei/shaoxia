from sqlalchemy import (
    MetaData, Table, Column, ForeignKey,
    Integer, String, DateTime, Text, create_engine, insert
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from settings import config

meta = MetaData()

user = Table(
    'user', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(100), nullable=False),
    Column('passwd', String(100), nullable=False),
    Column('email', String(100), nullable=False),
    Column('authority', String(10), nullable=False), # admin or user
    mysql_engine='InnoDB',
    mysql_charset='utf8mb4'
)

login_records = Table(
    'login_records', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('user_id', Integer, ForeignKey('user.id'), nullable=False),
    Column('login_time', DateTime(timezone=True), default=func.now()),
    Column('token', String(30), nullable=False),
    Column('logout_time', DateTime(timezone=True), nullable=False),
    Column('login_ip', String(100), nullable=False),
    mysql_engine='InnoDB',
    mysql_charset='utf8mb4'
)

upload_data = Table(
    'upload_data', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('user_id', Integer, ForeignKey('user.id'), nullable=False),
    Column('upload_time', DateTime(timezone=True), default=func.now()),
    Column('upload_path', String(300), nullable=False),
    Column('sequencing_type', String(10), nullable=False), # scRNA scATAC scST
    Column('data_type', String(10), nullable=False), # matrix cellranger fastq
    Column('data_note', String(100), nullable=False),
    Column('species', String(10), nullable=False), # human or mouse
    Column('tissue', String(10), nullable=False), # need refine
    mysql_engine='InnoDB',
    mysql_charset='utf8mb4'
)

upstream_reads_mapping = Table(
    'upstream_reads_mapping', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('upload_data_id', Integer, ForeignKey('upload_data.id'), nullable=False),
    Column('slurm_job_id', Integer, nullable=False),
    Column('slurm_job_state', String(50), nullable=False),
    Column('result_path', String(300), nullable=False), # outs/filtered_feature_bc_matrix
    mysql_engine='InnoDB',
    mysql_charset='utf8mb4'
)

upstream_generate_loom = Table(
    'upstream_generate_loom', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('upstream_reads_mapping_id', Integer, ForeignKey('upstream_reads_mapping.id'), nullable=False),
    Column('slurm_job_id', Integer, nullable=False),
    Column('slurm_job_state', String(50), nullable=False),
    Column('result_path', String(300), nullable=False), # upstream_reads_mapping.result_path/../../velocyto
    mysql_engine='InnoDB',
    mysql_charset='utf8mb4'
)

upstream_quality_control = Table(
    'upstream_quality_control', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('input_data_id', Integer, nullable=False), # upstream_reads_mapping or upload_data or upstream_generate_loom id
    Column('input_data_type', String(50), nullable=False), # upstream_reads_mapping or upload_data_cellranger or upload_data_matrix or upstream_generate_loom
    Column('r_code', Text, nullable=False),
    Column('result_path', String(300), nullable=False),
    mysql_engine='InnoDB',
    mysql_charset='utf8mb4'
)

# 只能 merge 同一个物种的 cellranger 结果
upstream_merge = Table(
    'upstream_merge', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('user_id', Integer, ForeignKey('user.id'), nullable=False),
    Column('input_qc_ids', String(50), nullable=False), # upstream_quality_control.id, sep = ':'
    Column('input_data_ids', String(50), nullable=False),# upload_data.id, sep = ':'
    Column('r_code', Text, nullable=False),
    mysql_engine='InnoDB',
    mysql_charset='utf8mb4'
)

# 只能 integrate 同一个物种的 cellranger 结果
upstream_integrate = Table(
    'upstream_integrate', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('user_id', Integer, ForeignKey('user.id'), nullable=False),
    Column('input_qc_ids', String(50), nullable=False), # upstream_quality_control.id, sep = ':'
    Column('input_data_ids', String(50), nullable=False),# upload_data.id, sep = ':'
    Column('r_code', Text, nullable=False),
    mysql_engine='InnoDB',
    mysql_charset='utf8mb4'
)

upstream_dc = Table(
    'upstream_dc', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('input_data_id', Integer, nullable=False), # upstream_quality_control or upstream_merge or upstream_integrate id
    Column('input_data_type', String(50), nullable=False), # upstream_quality_control or upstream_merge or upstream_integrate
    Column('n_cluster', Integer, nullable=False),
    Column('result_path', String(300), nullable=False),
    Column('r_code', Text, nullable=False),
    mysql_engine='InnoDB',
    mysql_charset='utf8mb4'
)

annotation_type = Table(
    'annotation_type', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('user_id', Integer, ForeignKey('user.id'), nullable=False),
    Column('upstream_dc_id', Integer, ForeignKey('upstream_dc.id'), nullable=False),
    Column('str_anno', Text, nullable=False),
    Column('r_code', Text, nullable=False),
    Column('marker_gene_order', Text, nullable=False),
    Column('cluster_name_order', Text, nullable=False),
    Column('result_path', String(300), nullable=False),
    mysql_engine='InnoDB',
    mysql_charset='utf8mb4'
)

annotation_subtype = Table(
    'annotation_subtype', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('user_id', Integer, ForeignKey('user.id'), nullable=False),
    Column('annotation_type_id', Integer, ForeignKey('annotation_type.id'), nullable=False),
    Column('input_cell_name', String(50), nullable=False),
    Column('str_anno', Text, nullable=False),
    Column('dc_r_code', Text, nullable=False),
    Column('anno_r_code', Text, nullable=False),
    Column('n_cluster', Integer, nullable=False),
    Column('marker_gene_order', Text, nullable=False),
    Column('cluster_name_order', Text, nullable=False),
    Column('result_path', String(300), nullable=False),
    mysql_engine='InnoDB',
    mysql_charset='utf8mb4'
)

downstream = Table(
    'downstream', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('user_id', Integer, ForeignKey('user.id'), nullable=False),
    Column('annotation_id', Integer, nullable=False), # annotation_type.id or annotation_subtype.id
    Column('input_data_type', String(50), nullable=False), # annotation_type or annotation_subtype
    Column('slurm_job_id', Integer, nullable=False),
    Column('slurm_job_state', String(50), nullable=False),
    Column('analysis_type', String(50), nullable=False),
    Column('result_path', String(300), nullable=False),
    mysql_engine='InnoDB',
    mysql_charset='utf8mb4'
)

# analysis type: 1) inferCNV 2) correlation 3) cellCycleScore 4) cellFrequency
# 5) GO_KEGG_erichment 6) GSEA 7) GSVA 8) velocity 9) trajectory 10) cellCommunication
# 11) GRN

if __name__ == '__main__':
    # create tables
    auser = str(config["MysqlUser"])
    passwd = str(config["MysqlPassword"])
    host = str(config["MysqlHost"])
    database = str(config["MysqlDatabase"])
    url = "mysql+pymysql://{}:{}@{}/{}".format(auser, passwd, host, database)
    engine = create_engine(url=url)
    meta.create_all(engine)
    # insert admin user to user table
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    insert_stmt = insert(user).values(
        name=str(config["adminName"]),
        passwd=str(config["adminPasswd"]),
        email=str(config["adminEmail"]),
        authority="admin"
    )
    session.execute(insert_stmt)
    session.commit()
    session.close()