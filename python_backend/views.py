from aiohttp import web
from db import (
    user, login_records, upload_data, upstream_reads_mapping,
    upstream_reads_mapping, upstream_quality_control, upstream_merge,
    upstream_integrate, upstream_dc, annotation_type, annotation_subtype,
    downstream, upstream_generate_loom
)
from sqlalchemy.sql import func
from shutil import rmtree
from scripts import (
    cellranger_script, r_script_inferCNV, downstream_bash,
    r_script_correlation, r_script_GSEA, r_script_trajectory_monocle2, r_script_trajectory_monocle3,
    generate_loom_script, r_script_cellphoneDB, read_loom_r_code,
    r_script_velocity, seurat_DoHeatmap, seurat_VlnPlot,
    r_script_cell_frequency, r_script_enrichment, py_script_velocity,
    r_script_grn, r_script_GSVA, r_script_cell_cycle, r_script_visualizing_cellphoneDB
)
from settings import config
import pyRserve

import random
import string
import pathlib
import asyncio
import glob
import aiofiles
import uuid
import subprocess

lock_user = asyncio.Lock()
logined_user = {} # key: user_id  value: token
lock_rserv = asyncio.Lock() # lock for rserv_connections
rserv_connections = {} # key: user_id:job_type:data_id  value: Rserve_connection
lock = asyncio.Lock() # lock for slurm_jobs
slurm_jobs = {} # key: slurm_job_id  value: job_types(reads_mapping downstream)

async def query_slurm_job_state(app):
    while True:
        await asyncio.sleep(600) # run one time per 10 min 
        async with lock:
            keys_to_del = []
            for key in slurm_jobs:
                """
                #output = subprocess.check_output(["/usr/local/bin/sacct", "-X", "-ostate", "-j", str(key)])
                proc = await asyncio.create_subprocess_exec("/usr/local/bin/sacct", "-X", "-ostate", "-j", str(key),
                        stdout=asyncio.subprocess.PIPE)
                stdout, stderr = await proc.communicate()
                output = stdout.strip().decode('utf-8')
                """
                # get job state from slurm database
                async with app['slurm_db'].acquire() as conn_slurm:
                    sql = '''
                        SELECT state FROM scap_job_table
                        WHERE id_job = $job_id;
                    '''
                    sql = sql.replace('$job_id', str(key))
                    res = await conn_slurm.execute(sql)
                    row = await res.fetchone()
                    state = row[0]
                
                '''
                enum job_states {
                    JOB_PENDING, /*0 queued waiting for initiation */
                    JOB_RUNNING, /*1 allocated resources and executing */
                    JOB_SUSPENDED, /*2 allocated resources, execution suspended */
                    JOB_COMPLETE, /*3 completed execution successfully */
                    JOB_CANCELLED, /*4 cancelled by user */
                    JOB_FAILED, /5* completed execution unsuccessfully */
                    JOB_TIMEOUT, /*6 terminated on reaching time limit */
                    JOB_NODE_FAIL, /*7 terminated on node failure */
                    JOB_PREEMPTED, /*8 terminated due to preemption */
                    JOB_END /*9 not a real state, last entry in table */
                };
                '''
                async with app['mysql_db'].acquire() as conn:
                    async with conn.begin() as transaction:
                        if state == 3 : 
                            if slurm_jobs[key] == 'generate_loom':
                                await conn.execute(upstream_generate_loom.update()
                                    .where(upstream_generate_loom.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='COMPLETED'
                                    )
                                )
                            elif slurm_jobs[key] == 'reads_mapping':
                                await conn.execute(upstream_reads_mapping.update()
                                    .where(upstream_reads_mapping.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='COMPLETED'
                                    )
                                )
                            else:
                                await conn.execute(downstream.update()
                                    .where(downstream.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='COMPLETED'
                                    )
                                )

                            # delete completed slurm job id from slurm_jobs
                            keys_to_del.append(key)

                        elif state == 2 :
                            if slurm_jobs[key] == 'generate_loom':
                                await conn.execute(upstream_generate_loom.update()
                                    .where(upstream_generate_loom.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='SUSPENDED'
                                    )
                                )
                            elif slurm_jobs[key] == 'reads_mapping':
                                await conn.execute(upstream_reads_mapping.update()
                                    .where(upstream_reads_mapping.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='SUSPENDED'
                                    )
                                )
                            else:
                                await conn.execute(downstream.update()
                                    .where(downstream.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='SUSPENDED'
                                    )
                                )
                                
                        elif state == 5 :
                            if slurm_jobs[key] == 'generate_loom':
                                await conn.execute(upstream_generate_loom.update()
                                    .where(upstream_generate_loom.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='FAILED'
                                    )
                                )
                            elif slurm_jobs[key] == 'reads_mapping':
                                await conn.execute(upstream_reads_mapping.update()
                                    .where(upstream_reads_mapping.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='FAILED'
                                    )
                                )
                            else:
                                await conn.execute(downstream.update()
                                    .where(downstream.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='FAILED'
                                    )
                                )
                            # delete completed slurm job id from slurm_jobs
                            keys_to_del.append(key)

                        elif state == 4 :
                            if slurm_jobs[key] == 'generate_loom':
                                await conn.execute(upstream_generate_loom.update()
                                    .where(upstream_generate_loom.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='CANCELLED'
                                    )
                                )
                            elif slurm_jobs[key] == 'reads_mapping':
                                await conn.execute(upstream_reads_mapping.update()
                                    .where(upstream_reads_mapping.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='CANCELLED'
                                    )
                                )
                            else:
                                await conn.execute(downstream.update()
                                    .where(downstream.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='CANCELLED'
                                    )
                                )
                            # delete completed slurm job id from slurm_jobs
                            keys_to_del.append(key)
                        
                        elif state == 0:
                            pass

                        elif state == 6:
                            if slurm_jobs[key] == 'generate_loom':
                                await conn.execute(upstream_generate_loom.update()
                                    .where(upstream_generate_loom.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='TIMEOUT'
                                    )
                                )
                            elif slurm_jobs[key] == 'reads_mapping':
                                await conn.execute(upstream_reads_mapping.update()
                                    .where(upstream_reads_mapping.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='TIMEOUT'
                                    )
                                )
                            else:
                                await conn.execute(downstream.update()
                                    .where(downstream.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='TIMEOUT'
                                    )
                                )
                            # delete completed slurm job id from slurm_jobs
                            keys_to_del.append(key)

                        elif state == 7:
                            if slurm_jobs[key] == 'generate_loom':
                                await conn.execute(upstream_generate_loom.update()
                                    .where(upstream_generate_loom.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='NODE_FAIL'
                                    )
                                )
                            elif slurm_jobs[key] == 'reads_mapping':
                                await conn.execute(upstream_reads_mapping.update()
                                    .where(upstream_reads_mapping.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='NODE_FAIL'
                                    )
                                )
                            else:
                                await conn.execute(downstream.update()
                                    .where(downstream.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='NODE_FAIL'
                                    )
                                )
                            # delete completed slurm job id from slurm_jobs
                            keys_to_del.append(key)

                        elif state == 8:
                            if slurm_jobs[key] == 'generate_loom':
                                await conn.execute(upstream_generate_loom.update()
                                    .where(upstream_generate_loom.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='PREEMPTED'
                                    )
                                )
                            elif slurm_jobs[key] == 'reads_mapping':
                                await conn.execute(upstream_reads_mapping.update()
                                    .where(upstream_reads_mapping.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='PREEMPTED'
                                    )
                                )
                            else:
                                await conn.execute(downstream.update()
                                    .where(downstream.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='PREEMPTED'
                                    )
                                )
                            # delete completed slurm job id from slurm_jobs
                            keys_to_del.append(key)

                        elif state == 9:
                            pass

                        else: # state == 1
                            if slurm_jobs[key] == 'generate_loom':
                                await conn.execute(upstream_generate_loom.update()
                                    .where(upstream_generate_loom.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='RUNNING'
                                    )
                                )
                            elif slurm_jobs[key] == 'reads_mapping':
                                await conn.execute(upstream_reads_mapping.update()
                                    .where(upstream_reads_mapping.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='RUNNING'
                                    )
                                )
                            else:
                                await conn.execute(downstream.update()
                                    .where(downstream.c.slurm_job_id == key)
                                    .values(
                                        slurm_job_state='RUNNING'
                                    )
                                )

                        await transaction.commit()
            
            # do deletion out of loop
            for k in keys_to_del:
                del slurm_jobs[k]

async def index(request):
    s = open("../dist/index.html", "r")
    return web.Response(text=s.read(), content_type='text/html')

async def login(request):
    userInfo = {"user_name":"", "user_id":"", "is_admin": "n", "token": "", "is_ok": "n"}
    data = await request.post()
    email = data["email"]
    passwd = data["passwd"]
    async with request.app['mysql_db'].acquire() as conn:
        async with conn.begin() as transaction:
            res = await conn.execute(user.select()
                        .where((user.c.email == email) &
                               (user.c.passwd == passwd)
                        )
                    )
            row = await res.fetchone()
            # row is a tuple
            if row :
                userInfo["user_name"] = str(row["name"])
                userInfo["user_id"] = str(row["id"])
                userInfo["token"] = ''.join(random.sample(string.ascii_letters + string.digits, 25))
                if str(row["authority"]) == "admin":
                    userInfo["is_admin"] = "y"
                else:
                    userInfo["is_admin"] = "n"
                userInfo["is_ok"] = "y"
                # 
                async with lock_user:
                    global logined_user
                    logined_user[userInfo["user_id"]] = userInfo["token"]
                # 
                await conn.execute(login_records.insert().values(
                    user_id=row[0],
                    login_ip=request.remote,
                    token=userInfo["token"],
                    logout_time=func.now()
                ))
                await transaction.commit()

            return web.json_response(userInfo)

async def logout(request):
    data = await request.post()
    userID = str(data["user_id"])
    userToken = str(data["token"])
    async with lock_user:
        global logined_user
        del logined_user[userID]
    async with request.app['mysql_db'].acquire() as conn:
        async with conn.begin() as transaction:
            await conn.execute(login_records.update()
            .where(login_records.c.token == userToken)
            .values(logout_time = func.now()))
            await transaction.commit()
    return web.Response(text="OK")

async def upload(request):
    data = await request.post()
    user_id = str(data["user_id"])
    dataType = data["data_type"] # # matrix cellranger fastq

    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[user_id]:
            return web.Response(text="token error!", status=401)

    base_dir = pathlib.Path(config["dataDir"])
    upload = base_dir/ "upload" / "_".join(["user", user_id, 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    while upload.exists():
        upload = base_dir/ "upload" / "_".join(["user", user_id, 
                                                "".join(random.sample(string.ascii_letters + string.digits, 5))])
    await asyncio.to_thread(upload.mkdir, parents=True)

    if dataType == "cellranger":
        file_name = upload / data["barcode"].filename
        file_content = data["barcode"].file.read()
        async with aiofiles.open(file_name, 'wb') as f:
            await f.write(file_content)
            await f.close()

        file_name = upload / data["features"].filename
        file_content = data["features"].file.read()
        async with aiofiles.open(file_name, 'wb') as f:
            await f.write(file_content)
            await f.close()

        file_name = upload / data["matrix"].filename
        file_content = data["matrix"].file.read()
        async with aiofiles.open(file_name, 'wb') as f:
            await f.write(file_content)
            await f.close()

    elif dataType == "matrix":
        file_name = upload / "count_matrix.tsv" # rename upload count matrix file
        file_content = data["files"].file.read()
        async with aiofiles.open(file_name, 'wb') as f:
            await f.write(file_content)
            await f.close()
    else:
        for file in data["files"]:
            file_name = upload / file.filename
            file_content = data["files"].file.read()
            async with aiofiles.open(file_name, 'wb') as f:
                await f.write(file_content)
                await f.close()

    async with request.app['mysql_db'].acquire() as conn:
        async with conn.begin() as transaction:
            if dataType == "matrix":
                await conn.execute(upload_data.insert().values(
                    user_id = data["user_id"],
                    upload_path = str(upload/"count_matrix.tsv"), # matrix file full path
                    species = data["species"],
                    tissue = data["tissue"],
                    data_type = data["data_type"],
                    data_note = data["data_label"],
                    sequencing_type = data["sequecing_type"]
                ))
            else:
                await conn.execute(upload_data.insert().values(
                    user_id = data["user_id"],
                    upload_path = str(upload), # dir path
                    species = data["species"],
                    tissue = data["tissue"],
                    data_type = data["data_type"],
                    data_note = data["data_label"],
                    sequencing_type = data["sequecing_type"]
                ))
            await transaction.commit()

    return web.Response(text="Upload OK!")

####################################### upstream annalysis ###################################################

async def query_fastq_data(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    results = []
    async with request.app['mysql_db'].acquire() as conn:
        # 做了reads mapping 的数据，没有必要重新在做，用left join, 
        sql = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.species, a.tissue, a.data_note
            FROM   upload_data a
            LEFT JOIN upstream_reads_mapping b
            ON a.id = b.upload_data_id
            WHERE b.upload_data_id IS NULL AND a.data_type = 'fastq' AND a.user_id = $user_id;
        '''
        sql = sql.replace('$user_id', str(data["user_id"]))
        res = await conn.execute(sql)
        rows = await res.fetchall()
        for row in rows:
            results.append({
                "data_id": str(row[0]),
                "user_id": str(row[1]),
                "upload_time": str(row[2]),
                "sequencing_type": row[3],
                "data_type": row[4],
                "species": row[5],
                "tissue": row[6],
                "data_note": row[7]
            })

        return web.json_response(results)

async def rna_reads_mapping(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    async with request.app['mysql_db'].acquire() as conn:
        async with conn.begin() as transaction:
            res = await conn.execute(upload_data.select()
                .where(upload_data.c.id == data["data_id"])
            )
            row = await res.fetchone()

            # 生成 scirpt
            script = cellranger_script.replace("partition", config["slurmPartition"])
            script = script.replace("jobLogPath", config["slurmJobLogPath"])
            script = script.replace("useLocalCores", str(data["thread"]))

            base_dir = pathlib.Path(config["dataDir"]) / "upstream" / "cellranger"
            userPath = base_dir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
            while userPath.exists():
                userPath = base_dir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
            await asyncio.to_thread(userPath.mkdir, parents=True)

            script = script.replace("userDir", userPath.absolute().__str__())
            script = script.replace("cellRangerPath", config["cellRangerPath"])
            script = script.replace("uploadFastqPath", str(row["upload_path"]))

            if row['species'] == "human":
                script = script.replace("TranscriptomePath", config["humanTranscriptomePath"])
            else:
                script = script.replace("TranscriptomePath", config["mounseTranscriptomePath"])
            
            script = script.replace("useLocalMem", str(data["memory"]))

            # using aiofiles for non-blocking file IOs: 输出 script 到文件， 再提交slurm任务
            async with aiofiles.tempfile.NamedTemporaryFile(
                mode = 'w',
                prefix="rna_mapping",
                suffix=".sh",
                dir=config['slurmJobScriptPath'],
                delete=False) as file:
                
                await file.write(script)

            output = await asyncio.to_thread(subprocess.check_output, ["/usr/local/bin/sbatch", str(file.name)])
            """
            cmd = "sbatch "+file.name
            proc = await asyncio.create_subprocess_shell(
                cmd, 
                stdout=asyncio.subprocess.PIPE, 
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            x = stdout.decode().split()
            """
            x = output.decode().split()
            jobID = int(x[-1])
            resultPath = userPath.absolute().__str__() + '/cellranger/outs/filtered_feature_bc_matrix'

            async with lock:
                slurm_jobs[jobID] = 'reads_mapping'

            await conn.execute(upstream_reads_mapping.insert().values(
                upload_data_id = data["data_id"],
                slurm_job_id = jobID,
                result_path = resultPath,
                slurm_job_state = 'PENDING'
            ))
            await transaction.commit()

            return web.Response(text="OK")

async def query_generate_loom_data(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    results = []
    async with request.app['mysql_db'].acquire() as conn:
        # 做了generate_loom 的数据，没有必要重新在做，用inner join, 然后 left join
        sql = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.species, a.tissue, a.data_note, b.id
            FROM   upload_data a INNER JOIN upstream_reads_mapping b
            ON a.id = b.upload_data_id
            LEFT JOIN upstream_generate_loom c 
            ON b.id = c.upstream_reads_mapping_id
            WHERE c.upstream_reads_mapping_id IS NULL AND b.slurm_job_state = 'COMPLETED' AND a.user_id = $user_id;
        '''
        sql = sql.replace('$user_id', str(data["user_id"]))
        res = await conn.execute(sql)
        rows = await res.fetchall()
        for row in rows:
            results.append({
                "data_id": str(row[0]),
                "user_id": str(row[1]),
                "upload_time": str(row[2]),
                "sequencing_type": row[3],
                "data_type": row[4],
                "species": row[5],
                "tissue": row[6],
                "data_note": row[7],
                "reads_mapping_id": row[8]
            })

        return web.json_response(results)
 
async def generate_loom(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    async with request.app['mysql_db'].acquire() as conn:
        async with conn.begin() as transaction:
            res = await conn.execute(upstream_reads_mapping.select()
                .where(upstream_reads_mapping.c.id == data["reads_mapping_id"])
            )
            row = await res.fetchone()

            script = generate_loom_script.replace("partition", config["slurmPartition"])
            script = script.replace("jobLogPath", config["slurmJobLogPath"])

            if data['species'] == "human":
                script = script.replace("transcriptomePath", config["humanTranscriptomePath"])
            else:
                script = script.replace("transcriptomePath", config["mounseTranscriptomePath"])

            readsMappingResultPath = pathlib.Path(row['result_path']).parent.parent.absolute().__str__()
            script = script.replace("readsMappingResultPath",readsMappingResultPath)

            # 输出 script 到文件， 在提交slurm任务
            async with aiofiles.tempfile.NamedTemporaryFile(
                mode="w",
                prefix="rna_generate_loom",
                suffix=".sh",
                dir=config['slurmJobScriptPath'],
                delete=False
            ) as file:
                await file.write(script)

            """cmd = "sbatch "+file.name
            proc = await asyncio.create_subprocess_shell(
                cmd, 
                stdout=asyncio.subprocess.PIPE, 
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            """
            output = await asyncio.to_thread(subprocess.check_output, ["/usr/local/bin/sbatch", str(file.name)])
            x = output.decode().split()
            jobID = int(x[-1])
            resultPath = pathlib.Path(row['result_path']).parent.parent / "velocyto"

            async with lock:
                slurm_jobs[jobID] = 'generate_loom'

            await conn.execute(upstream_generate_loom.insert().values(
                upstream_reads_mapping_id = data["reads_mapping_id"],
                slurm_job_id = jobID,
                result_path = resultPath.absolute().__str__(),
                slurm_job_state = 'PENDING'
            ))
            await transaction.commit()

            return web.Response(text="OK")

async def query_quality_control_data(request): 
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)

    async with request.app['mysql_db'].acquire() as conn:
        results  = [] # list of dic
        # 1. check upload data table for cellranger data upload
        res = await conn.execute(upload_data.select()
                .where(upload_data.c.data_type == "cellranger")
                .where(upload_data.c.user_id == data["user_id"])
            )

        async for row in res:
            results.append({
                "data_id": str(row[0]),
                "user_id": str(row[1]),
                "upload_time": str(row[2]),
                "sequencing_type": row[4],
                "data_type": row[5],
                "data_note": row[6],
                "species": row[7],
                "tissue": row[8] ,
                "is_reads_mapping": "-",
                "reads_mapping_id": "-",
                "is_generate_loom": "-",
                "generate_loom_id": "-"
            })

        # 2. check upload_data tabel for matrix data upload
        res1 = await conn.execute(upload_data.select()
            .where(upload_data.c.data_type == "matrix")
            .where(upload_data.c.user_id == data["user_id"]))

        async for row in res1:
            results.append({
                "data_id": str(row[0]),
                "user_id": str(row[1]),
                "upload_time": str(row[2]),
                "sequencing_type": row[4],
                "data_type": row[5],
                "data_note": row[6],
                "species": row[7],
                "tissue": row[8] ,
                "is_reads_mapping": "-",
                "reads_mapping_id": "-",
                "is_generate_loom": "-",
                "generate_loom_id": "-"
            })

        # 3. check reads mapping result table
        sql = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.data_note, a.species, a.tissue, b.id
            FROM upload_data a, upstream_reads_mapping b
            WHERE a.id = b.upload_data_id AND b.slurm_job_state = 'COMPLETED' AND a.user_id = $user_id;
        '''
        sql = sql.replace("$user_id", str(data["user_id"]))
        res2 = await conn.execute(sql)
        async for row in res2:
            results.append({
                "data_id": str(row[0]),
                "user_id": str(row[1]),
                "upload_time": str(row[2]),
                "sequencing_type": row[3],
                "data_type": row[4],
                "data_note": row[5],
                "species": row[6],
                "tissue": row[7] ,
                "is_reads_mapping": "Y",
                "reads_mapping_id": str(row[8]),
                "is_generate_loom": "-",
                "generate_loom_id": "-"
            })
        # 4. check generate loom table
        sql1 = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.data_note, a.species, a.tissue, b.id, c.id
            FROM upload_data a, upstream_reads_mapping b, upstream_generate_loom c
            WHERE a.id = b.upload_data_id AND b.slurm_job_state = 'COMPLETED'
            AND c.slurm_job_state = 'COMPLETED' AND a.user_id = $user_id;
        '''
        sql1 = sql1.replace("$user_id", str(data["user_id"]))
        res3 = await conn.execute(sql1)
        async for row in res3:
            results.append({
                "data_id": str(row[0]),
                "user_id": str(row[1]),
                "upload_time": str(row[2]),
                "sequencing_type": row[3],
                "data_type": row[4],
                "data_note": row[5],
                "species": row[6],
                "tissue": row[7] ,
                "is_reads_mapping": "Y",
                "reads_mapping_id": str(row[8]),
                "is_generate_loom": "Y",
                "generate_loom_id": str(row[9])
            })        
        return web.json_response(results)

async def show_RNA_qc_image(request):
    data = await request.post()
    async with lock_user:
        if str(data["user_id"]) not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    # 1. check if user has a Rserv connection
    
    sKey = str(data["user_id"]) + ':' + 'qc'+ ':'+ str(data["data_id"])
    rbaseName = 'seurat_'+str(data["data_id"]) # R variable name
    if sKey not in rserv_connections:
        conn = await asyncio.to_thread(pyRserve.connect)
        async with lock_rserv:
            rserv_connections[sKey] = conn
        # load R script header code
        await asyncio.to_thread(conn.voidEval, 'setwd("'+config["tempImageDir"]+'")')
        await asyncio.to_thread(conn.voidEval, 'library(Seurat)')
        await asyncio.to_thread(conn.voidEval, 'library(SeuratObject)')
        await asyncio.to_thread(conn.voidEval, 'library(ggplot2)')
        await asyncio.to_thread(conn.voidEval, 'library(cowplot)')
        await asyncio.to_thread(conn.voidEval, "library(SeuratWrappers)")
        await asyncio.to_thread(conn.voidEval, "library(SeuratDisk)")
        await asyncio.to_thread(conn.voidEval, read_loom_r_code)

        if data["is_generate_loom"] == "Y":
            async with request.app['mysql_db'].acquire() as con:
                res = await con.execute(upstream_generate_loom.select()
                    .where(upstream_generate_loom.c.id == data["generate_loom_id"])
                )
                row = await res.fetchone()
                loom_file = row['result_path'] + '/cellranger.loom'
                await asyncio.to_thread(conn.voidEval, rbaseName+' <- read.loom.matrices(file = "'+loom_file+'")')
                await asyncio.to_thread(conn.voidEval, rbaseName+' <- as.Seurat('+rbaseName+')')
                await asyncio.to_thread(conn.voidEval, rbaseName+'[["RNA"]] <- '+rbaseName+'[["spliced"]]')
                await asyncio.to_thread(conn.voidEval, 'DefaultAssay('+rbaseName+') <- "RNA"')
        elif data["is_generate_loom"] == "-" and data["is_reads_mapping"] == 'Y':
            async with request.app['mysql_db'].acquire() as con:
                res = await con.execute(upstream_reads_mapping.select()
                    .where(upstream_reads_mapping.c.id == int(data["reads_mapping_id"]))
                )
                row = await res.fetchone()
                await asyncio.to_thread(conn.voidEval, rbaseName+'.data <- Read10X("'+row["result_path"]+'")')
                await asyncio.to_thread(conn.voidEval, rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)')        
                
        elif data["is_generate_loom"] == "-" and data["is_reads_mapping"] == '-' and data["data_type"] == "matrix":
            async with request.app['mysql_db'].acquire() as con:
                res = await con.execute(upload_data.select()
                    .where(upload_data.c.id == int(data["data_id"]))
                )
                row = await res.fetchone()
                await asyncio.to_thread(conn.voidEval, rbaseName+'.data <- read.table(file="'+row["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)')
                await asyncio.to_thread(conn.voidEval, rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)')
        else: #data["is_generate_loom"] == "-" and data["is_reads_mapping"] == '-' and data["data_type"] == "cellranger":
            async with request.app['mysql_db'].acquire() as con:
                res = await con.execute(upload_data.select()
                    .where(upload_data.c.id == int(data["data_id"]))
                )
                row = await res.fetchone()
                await asyncio.to_thread(conn.voidEval, rbaseName+'.data <- Read10X("'+row["upload_path"]+'")')
                await asyncio.to_thread(conn.voidEval, rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)')        
        
        await asyncio.to_thread(conn.voidEval, rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")')
        await asyncio.to_thread(conn.voidEval, rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")')
        # before qc image
        await asyncio.to_thread(conn.voidEval, 'p <- VlnPlot('+rbaseName+', features = c("nFeature_RNA", "nCount_RNA", "percent.mt", "percent.rb"), ncol = 3)')
        await asyncio.to_thread(conn.voidEval, 'p <- ggdraw(p) + draw_label(label= paste("cell number before quality control: ", length(colnames('+rbaseName+'))), size = 20, x = 0.7, y = 0.3)')
    else:
        conn = rserv_connections[sKey]
    
    # 2. output qc images
    beforeQC_png = "rna_before_qc_" + str(uuid.uuid1()) + ".png"
    afterQC_png = "rna_after_qc_" + str(uuid.uuid1()) + ".png"
    
    cmd = 'ggsave("'+beforeQC_png+'", p, width=10, height=6, units="in")'
    await asyncio.to_thread(conn.voidEval, cmd)

    # after qc image
    cmd = 'temp <- subset('+rbaseName+', subset = nFeature_RNA > '+data["LowGene"]+' & nFeature_RNA < '+data["UpGene"]+' & percent.mt < '+data["Mt"]+' & nCount_RNA > '+ data["Count"]+' & percent.rb > '+ data["Rb"]+')'
    await asyncio.to_thread(conn.voidEval, cmd)
    await asyncio.to_thread(conn.voidEval, 'p1 <- VlnPlot(temp, features = c("nFeature_RNA", "nCount_RNA", "percent.mt", "percent.rb"), ncol = 3)')
    await asyncio.to_thread(conn.voidEval, 'p1 <- ggdraw(p1) + draw_label(label= paste("cell number after quality control: ", length(colnames(temp))), size = 20, x = 0.7, y = 0.3)')
    cmd = 'ggsave("'+afterQC_png+'", p1, width=10, height=6, units="in")'
    await asyncio.to_thread(conn.voidEval, cmd)
    result = [{'id':0, 'url': "/tmpImg/"+beforeQC_png},{'id':1, 'url': "/tmpImg/"+afterQC_png}]
    return web.json_response(result)

async def scRNA_quality_control(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    sKey = str(data["user_id"]) + ':' + 'qc'+ ':'+ str(data["data_id"])
    rbaseName = 'seurat_'+str(data["data_id"]) # R variable name

    base_dir = pathlib.Path(config["dataDir"]) / "upstream" / "seurat"
    userPath = base_dir / "_".join(["user", str(data["user_id"]),
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    while userPath.exists():
        userPath = base_dir / "_".join(["user", str(data["user_id"]),
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    await asyncio.to_thread(userPath.mkdir, parents=True)

    if data["is_generate_loom"] == "Y":
        data_type = 'upstream_generate_loom'
        data_id = int(data["generate_loom_id"])
    elif data["is_generate_loom"] == "-" and data["is_reads_mapping"] == 'Y':
        data_type = 'upstream_reads_mapping'
        data_id = int(data["reads_mapping_id"])
    elif data["is_generate_loom"] == "-" and data["is_reads_mapping"] == '-' and data["data_type"] == "matrix":
        data_type = 'upload_data_matrix'
        data_id = int(data["data_id"])
    else:
        data_type = 'upload_data_cellranger'
        data_id = int(data["data_id"])

    # 1. generate qc images, and then close Rserv connection
    conn = rserv_connections[sKey]
    await asyncio.to_thread(conn.voidEval,'setwd("'+userPath.absolute().__str__()+'")')  
    await asyncio.to_thread(conn.voidEval,'p1 <- VlnPlot(temp, features = c("nFeature_RNA", "nCount_RNA", "percent.mt", "percent.rb"), ncol = 2)')
    await asyncio.to_thread(conn.voidEval,'ggsave("before_qc.png", p, width = 12, height = 11, dpi=600)')
    await asyncio.to_thread(conn.voidEval,'ggsave("after_qc.png", p1, width = 12, height = 11, dpi=600)')
    conn.close()
    async with lock_rserv:
        del rserv_connections[sKey]

    # 2. write code to database
    code_result = rbaseName+'<- subset('+rbaseName+', subset = nFeature_RNA > '+\
        data["LowGene"]+' & nFeature_RNA < '+data["UpGene"]+' & percent.mt < '+data["Mt"]+\
        ' & nCount_RNA > '+ data["Count"]+' & percent.rb > '+ data["Rb"]+')'
    
    async with request.app['mysql_db'].acquire() as conn:
        res = await conn.execute(upstream_quality_control.select()
                .where(upstream_quality_control.c.input_data_id == data_id)
                .where(upstream_quality_control.c.input_data_type == data_type)
            )
        row = await res.fetchone()
        if row:
            # 删除原本的质控结果
            rmtree(str(row["result_path"]))
            # 更新数据库中的质控结果
            async with conn.begin() as transaction:
                await conn.execute(upstream_dc.update()
                    .where(upstream_dc.c.input_data_id == data_id)
                    .where(upstream_quality_control.c.input_data_type == data_type)
                    .values(
                        r_code=code_result,
                        result_path = userPath.absolute().__str__()
                    )
                )
                await transaction.commit()
        else:
            async with conn.begin() as transaction:
                await conn.execute(upstream_quality_control.insert().values(
                            input_data_id=data_id,
                            input_data_type=data_type,
                            r_code=code_result,
                            result_path = userPath.absolute().__str__()
                    ))
                await transaction.commit()
        return web.Response(text="OK")

async def query_merge_data(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    results = []
    async with request.app['mysql_db'].acquire() as conn:
        # 1. fastq -> reads mapping -> generate_loom -> qc
        sql = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.data_note, a.species, a.tissue, b.id, c.id, d.id
            FROM upload_data a, upstream_reads_mapping b, upstream_generate_loom c, upstream_quality_control d
            WHERE a.id = b.upload_data_id 
                  AND d.input_data_type = 'upstream_generate_loom'
                  AND c.id = d.input_data_id
                  AND a.user_id = $user_id;
        '''
        sql = sql.replace("$user_id", str(data["user_id"]))
        res = await conn.execute(sql)
        async for row in res:
            results.append({
                "data_id": str(row[0]),
                "user_id": str(row[1]),
                "upload_time": str(row[2]),
                "sequencing_type": row[3],
                "data_type": row[4],
                "data_note": row[5],
                "species": row[6],
                "tissue": row[7] ,
                "is_reads_mapping": "Y",
                "reads_mapping_id": str(row[8]),
                "is_generate_loom": "Y",
                "generate_loom_id": str(row[9]),
                "is_quality_control": "Y",
                "quality_control_id": str(row[10])
            })
        # 2. fastq -> reads mapping -> qc
        sql1 = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.data_note, a.species, a.tissue, b.id, c.id
            FROM upload_data a, upstream_reads_mapping b, upstream_quality_control c
            WHERE a.id = b.upload_data_id
                  AND c.input_data_type = 'upstream_reads_mapping'
                  AND b.id = c.input_data_id
                  AND a.user_id = $user_id;
        '''
        sql1 = sql1.replace("$user_id", str(data["user_id"]))        
        res1 = await conn.execute(sql1)
        async for row1 in res1:
            results.append({
                "data_id": str(row1[0]),
                "user_id": str(row1[1]),
                "upload_time": str(row1[2]),
                "sequencing_type": row1[3],
                "data_type": row1[4],
                "data_note": row1[5],
                "species": row1[6],
                "tissue": row1[7] ,
                "is_reads_mapping": "Y",
                "reads_mapping_id": str(row1[8]),
                "is_generate_loom": "-",
                "generate_loom_id": "-",
                "is_quality_control": "Y",
                "quality_control_id": str(row1[9])
            })
        # 3. matrix -> qc
        sql2 = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.data_note, a.species, a.tissue, b.id
            FROM upload_data a, upstream_quality_control b
            WHERE a.id = b.input_data_id
                  AND b.input_data_type = 'upload_data_matrix' 
                  AND a.user_id = $user_id;
        '''
        sql2 = sql2.replace("$user_id", str(data["user_id"]))        
        res2 = await conn.execute(sql2)
        async for row2 in res2:
            results.append({
                "data_id": str(row2[0]),
                "user_id": str(row2[1]),
                "upload_time": str(row2[2]),
                "sequencing_type": row2[3],
                "data_type": row2[4],
                "data_note": row2[5],
                "species": row2[6],
                "tissue": row2[7] ,
                "is_reads_mapping": "-",
                "reads_mapping_id": "-",
                "is_generate_loom": "-",
                "generate_loom_id": "-",
                "is_quality_control": "Y",
                "quality_control_id": str(row2[8])
            })

        # 4. cellranger -> qc
        sql3 = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.data_note, a.species, a.tissue, b.id
            FROM upload_data a, upstream_quality_control b
            WHERE a.id = b.input_data_id 
                  AND b.input_data_type = 'upload_data_cellranger' 
                  AND a.user_id = $user_id;
        '''
        sql3 = sql3.replace("$user_id", str(data["user_id"]))        
        res3 = await conn.execute(sql3)
        async for row3 in res3:
            results.append({
                "data_id": str(row3[0]),
                "user_id": str(row3[1]),
                "upload_time": str(row3[2]),
                "sequencing_type": row3[3],
                "data_type": row3[4],
                "data_note": row3[5],
                "species": row3[6],
                "tissue": row3[7] ,
                "is_reads_mapping": "-",
                "reads_mapping_id": "-",
                "is_generate_loom": "-",
                "generate_loom_id": "-",
                "is_quality_control": "Y",
                "quality_control_id": str(row3[8])
            })
        
        return web.json_response(results)

async def scRNA_merge(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    code_result = ""
    data_ids = str(data["data_ids"]).replace(',', ':') 
    qc_ids = str(data["qc_ids"]).replace(',', ':')
    x = data_ids.split(':')
    xx = str(data["group_names"]).replace(',', ':').split(':')
    if len(x) == 2 :
        code_result = 'project.combined <- merge('+'seurat_'+x[0]+', y='+'seurat_'+x[1]+', add.cell.ids = c("'+xx[0]+'", "'+xx[1]+'"), project = "merge_project")'
    else:
        c = ''
        c2 = ''
        for index, item in enumerate(x):
            if index != 0:
                if index == len(x)-1:
                    c += 'seurat_'+item
                else:
                    c += 'seurat_'+item+', '

        for ii, it in enumerate(xx):
            if ii == len(xx)-1:
                c2 += '"'+it+'"'
            else:
                c2 += '"'+it+'",'
        
        code_result = 'project.combined <- merge('+'seurat_'+x[0]+', y=c('+c+'), add.cell.ids = c('+c2+'), project = "merge_project")'
        
    # for group naming
    group_type_R_code = '\nproject.combined$type <- ""\n'
    template_type = 'project.combined$type[project.combined$orig.ident == "$seurat"] = "$group"\n'

    for n, _ in enumerate(xx):
        group_type_R_code += template_type.replace("$seurat", 'seurat_'+x[n]).replace("$group", xx[n])
        
    code_result += group_type_R_code


    async with request.app['mysql_db'].acquire() as conn:
        async with conn.begin() as transaction:
            await conn.execute(upstream_merge.insert().values(
                user_id = int(data["user_id"]),
                input_data_ids = data_ids,
                input_qc_ids = qc_ids,
                r_code = code_result
            ))
            await transaction.commit()

            return web.Response(text="OK")

async def scRNA_integrate(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    data_ids = str(data["data_ids"]).replace(',', ':') 
    qc_ids = str(data["qc_ids"]).replace(',', ':')
    x = data_ids.split(':')
    xx = str(data["group_names"]).replace(',', ':').split(':')

    c = ''
    for index, item in enumerate(x):
        if index == len(x)-1:
            c += 'seurat_'+item
        else:
            c += 'seurat_'+item+', '

    code_result = 'ifnb.list <- list('+c+')\n' + \
    'ifnb.list <- lapply(X = ifnb.list, FUN = SCTransform)\n' + \
    'features <- SelectIntegrationFeatures(object.list = ifnb.list, nfeatures = 30000)\n' + \
    'ifnb.list <- PrepSCTIntegration(object.list = ifnb.list, anchor.features = features)\n' + \
    'anchors <- FindIntegrationAnchors(object.list = ifnb.list, normalization.method = "SCT", anchor.features = features)\n' + \
    'project.combined <- IntegrateData(anchorset = anchors, normalization.method = "SCT")\n'

    # for group naming
    group_type_R_code = '\nproject.combined$type <- ""\n'
    template_type = 'project.combined$type[project.combined$orig.ident == "$seurat"] = "$group"\n'

    for n, _ in enumerate(xx):
        group_type_R_code += template_type.replace("$seurat", 'seurat_'+x[n]).replace("$group", xx[n])
        
    code_result += group_type_R_code

    async with request.app['mysql_db'].acquire() as conn:
        async with conn.begin() as transaction:
            await conn.execute(upstream_integrate.insert().values(
                user_id = int(data["user_id"]),
                input_data_ids = data_ids,
                input_qc_ids = qc_ids,
                r_code = code_result
            ))
            await transaction.commit()

            return web.Response(text="OK")

async def query_dc_data(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    async with request.app['mysql_db'].acquire() as conn:
        results = []
        # 1. merge data
        # 2. integrate data
        # 3. fastq -> reads mapping -> generate_loom -> qc
        # 4. fastq -> reads mapping -> qc
        # 5. matrix -> qc & cellranger -> qc

        # 1. merge data
        resx = await conn.execute(upstream_merge.select()
            .where(upstream_merge.c.user_id == data["user_id"])
        )
        async for rowx in resx:
            results.append({
                "data_id": "-", 
                "user_id": "-",
                "upload_time": "-",
                "sequencing_type": "-",
                "data_type": "-",
                "data_note": "-",
                "species": "-",
                "tissue": "-" ,
                "is_reads_mapping": "-",
                "reads_mapping_id": "-",
                "is_generate_loom": "-",
                "generate_loom_id": "-",
                "is_quality_control": "-",
                "quality_control_id": "-",
                "is_combined": "Y",
                "combined_id": str(rowx[0]),
                "combined_type": "merge",
                "combined_data_id": str(rowx[3])
            })
        # 2. integrated data
        resxx = await conn.execute(upstream_integrate.select()
            .where(upstream_merge.c.user_id == data["user_id"])
        )
        async for rowxx in resxx:
            results.append({
                "data_id": "-", 
                "user_id": "-",
                "upload_time": "-",
                "sequencing_type": "-",
                "data_type": "-",
                "data_note": "-",
                "species": "-",
                "tissue": "-" ,
                "is_reads_mapping": "-",
                "reads_mapping_id": "-",
                "is_generate_loom": "-",
                "generate_loom_id": "-",
                "is_quality_control": "-",
                "quality_control_id": "-",
                "is_combined": "Y",
                "combined_id": str(rowxx[0]),
                "combined_type": "integrate",
                "combined_data_id": str(rowxx[3])
            })
        
        # 3. fastq -> reads mapping -> generate_loom -> qc
        sql = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.species, a.tissue, a.data_note, b.id, c.id, d.id
            FROM upload_data a, upstream_reads_mapping b, upstream_generate_loom c, upstream_quality_control d
            WHERE a.id = b.upload_data_id AND c.id = d.input_data_id 
            AND d.input_data_type = 'upstream_generate_loom' AND a.user_id = $user_id;
        '''
        sql = sql.replace("$user_id", str(data["user_id"]))
        res = await conn.execute(sql)
        async for row in res:
            results.append({
                "data_id": str(row[0]),
                "user_id": str(row[1]),
                "upload_time": str(row[2]),
                "sequencing_type": row[3],
                "data_type": row[4],
                "data_note": row[5],
                "species": row[6],
                "tissue": row[7] ,
                "is_reads_mapping": "Y",
                "reads_mapping_id": str(row[8]),
                "is_generate_loom": "Y",
                "generate_loom_id": str(row[9]),
                "is_quality_control": "Y",
                "quality_control_id": str(row[10]),
                "is_combined": "-",
                "combined_id": "-",
                "combined_type": "-",
                "combined_data_id": "-"
            })
        # 4. fastq -> reads mapping -> qc
        sql1 = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.species, a.tissue, a.data_note, b.id, c.id
            FROM upload_data a, upstream_reads_mapping b, upstream_quality_control c
            WHERE a.id = b.upload_data_id AND b.id = c.input_data_id
            AND c.input_data_type = 'upstream_reads_mapping'  AND a.user_id = $user_id;
        '''
        sql1 = sql1.replace("$user_id", str(data["user_id"]))
        res1 = await conn.execute(sql1)
        async for row1 in res1:
            results.append({
                "data_id": str(row1[0]),
                "user_id": str(row1[1]),
                "upload_time": str(row1[2]),
                "sequencing_type": row1[3],
                "data_type": row1[4],
                "data_note": row1[5],
                "species": row1[6],
                "tissue": row1[7] ,
                "is_reads_mapping": "Y",
                "reads_mapping_id": str(row1[8]),
                "is_generate_loom": "-",
                "generate_loom_id": "-",
                "is_quality_control": "Y",
                "quality_control_id": str(row1[9]),
                "is_combined": "-",
                "combined_id": "-",
                "combined_type": "-",
                "combined_data_id": "-",
            })
        # 5. matrix -> qc
        sql2 = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.data_note, a.species, a.tissue, b.id
            FROM upload_data a, upstream_quality_control b
            WHERE a.id = b.input_data_id AND b.input_data_type = 'upload_data_matrix'
            AND a.user_id = $user_id;
        '''
        sql2 = sql2.replace("$user_id", str(data["user_id"]))        
        res2 = await conn.execute(sql2)
        async for row2 in res2:
            results.append({
                "data_id": str(row2[0]),
                "user_id": str(row2[1]),
                "upload_time": str(row2[2]),
                "sequencing_type": row2[3],
                "data_type": row2[4],
                "data_note": row2[5],
                "species": row2[6],
                "tissue": row2[7] ,
                "is_reads_mapping": "-",
                "reads_mapping_id": "-",
                "is_generate_loom": "-",
                "generate_loom_id": "-",
                "is_quality_control": "Y",
                "quality_control_id": str(row2[8]),
                "is_combined": "-",
                "combined_id": "-",
                "combined_type": "-",
                "combined_data_id": "-",
            })

        # 6. cellranger -> qc
        sql3 = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.data_note, a.species, a.tissue, b.id
            FROM upload_data a, upstream_quality_control b
            WHERE a.id = b.input_data_id AND b.input_data_type = 'upload_data_cellranger'
            AND a.user_id = $user_id;
        '''
        sql3 = sql3.replace("$user_id", str(data["user_id"]))        
        res3 = await conn.execute(sql3)
        async for row3 in res3:
            results.append({
                "data_id": str(row3[0]),
                "user_id": str(row3[1]),
                "upload_time": str(row3[2]),
                "sequencing_type": row3[3],
                "data_type": row3[4],
                "data_note": row3[5],
                "species": row3[6],
                "tissue": row3[7] ,
                "is_reads_mapping": "-",
                "reads_mapping_id": "-",
                "is_generate_loom": "-",
                "generate_loom_id": "-",
                "is_quality_control": "Y",
                "quality_control_id": str(row3[8]),
                "is_combined": "-",
                "combined_id": "-",
                "combined_type": "-",
                "combined_data_id": "-",
            })
        
        return web.json_response(results)

async def show_scRNA_dc_image(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    # 1. check if user has a Rserv connection
    
    rbaseName = '' # R variable name
    if data["is_combined"] == "Y":
        rbaseName = "project.combined"
        sKey = str(data["user_id"]) + ':' + 'dc:combine'+ ':'+ str(data["combined_id"])
    else:
        rbaseName = 'seurat_'+str(data["data_id"])
        sKey = str(data["user_id"]) + ':' + 'dc'+ ':'+ str(data["data_id"])
    
    if sKey not in rserv_connections:
        con = await asyncio.to_thread(pyRserve.connect)
        async with lock_rserv:
            rserv_connections[sKey] = con

        # load data r code
        # query qc r code
        load_data_r_code = ''
        combined_r_code = ''
        qc_r_code = ''

        if  str(data["is_combined"]) == "Y" and str(data["combined_type"]) == "merge":
            async with request.app['mysql_db'].acquire() as conn:
                res = await conn.execute(upstream_merge.select()
                        .where(upstream_merge.c.id == int(data["combined_id"]))
                      )
                row = await res.fetchone()
                combined_r_code = row["r_code"]
            
                for x in str(row["input_qc_ids"]).split(':'):
                    res1 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.id == int(x))
                    )
                    row1 = await res1.fetchone()
                    qc_r_code += row1["r_code"] +";"
                
                for i in str(row["input_data_ids"]).split(':'):
                    res2 = await conn.execute(upload_data.select()
                        .where(upload_data.c.id == int(i))
                    )
                    row2 = await res2.fetchone()

                    if str(row2["data_type"]) == "cellranger":
                        load_data_r_code += 'seurat_'+i+'.data <- Read10X("'+str(row2["upload_path"])+'");'+\
                        'seurat_'+i+' <- CreateSeuratObject(counts=seurat_'+i+'.data,project = "seurat_'+i+'",min.cells=3,min.features=200);'+\
                        'seurat_'+i+'[["percent.mt"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^MT-");'+\
                        'seurat_'+i+'[["percent.rb"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^RP[SL]");'

                    elif str(row2["data_type"]) == "fastq":
                        res3 = await conn.execute(upstream_reads_mapping.select()
                                .where(upstream_reads_mapping.c.upload_data_id == int(i))
                            )
                        row3 = await res3.fetchone()
                        load_data_r_code += 'seurat_'+i+'.data <- Read10X("'+str(row3["result_path"])+'");'+\
                        'seurat_'+i+' <- CreateSeuratObject(counts=seurat_'+i+'.data,project = "seurat_'+i+'",min.cells=3,min.features=200);'+\
                        'seurat_'+i+'[["percent.mt"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^MT-");'+\
                        'seurat_'+i+'[["percent.rb"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^RP[SL]");'
                    else:
                        pass

        elif str(data["is_combined"]) == "Y" and str(data["combined_type"]) == "integrate":
            async with request.app['mysql_db'].acquire() as conn:
                res = await conn.execute(upstream_integrate.select()
                            .where(upstream_integrate.c.id == int(data["combined_id"]))
                    )
                row = await res.fetchone()
                combined_r_code = row["r_code"]
                                
                for x in str(row["input_qc_ids"]).split(':'):
                    res1 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.id == int(x))
                    )
                    row1 = await res1.fetchone()
                    qc_r_code += row1["r_code"]+";"
                
                for i in str(row["input_data_ids"]).split(':'):
                    res2 = await conn.execute(upload_data.select()
                        .where(upload_data.c.id == int(i))
                    )
                    row2 = await res2.fetchone()

                    if str(row2["data_type"]) == "cellranger":
                        load_data_r_code += 'seurat_'+i+'.data <- Read10X("'+str(row2["upload_path"])+'");'+\
                        'seurat_'+i+' <- CreateSeuratObject(counts=seurat_'+i+'.data,project = "seurat_'+i+'",min.cells=3,min.features=200);'+\
                        'seurat_'+i+'[["percent.mt"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^MT-");'+\
                        'seurat_'+i+'[["percent.rb"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^RP[SL]");'

                    elif str(row2["data_type"]) == "fastq":
                        res3 = await conn.execute(upstream_reads_mapping.select()
                                .where(upstream_reads_mapping.c.id == int(data["reads_mapping_id"]))
                            )
                        row3 = await res3.fetchone()
                        load_data_r_code += 'seurat_'+i+'.data <- Read10X("'+str(row3["result_path"])+'");'+\
                        'seurat_'+i+' <- CreateSeuratObject(counts=seurat_'+i+'.data,project = "seurat_'+i+'",min.cells=3,min.features=200);'+\
                        'seurat_'+i+'[["percent.mt"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^MT-");'+\
                        'seurat_'+i+'[["percent.rb"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^RP[SL]");'                        
                    else:
                        pass

        elif (str(data["is_combined"]) != "Y" and str(data["data_type"]) == "fastq"
                and str(data["is_reads_mapping"]) == "Y" and str(data["is_generate_loom"]) == "Y"
             ):    
            async with request.app['mysql_db'].acquire() as conn:
                res = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
                      )
                row = await res.fetchone()
                qc_r_code = row["r_code"]

                res1 = await conn.execute(upstream_generate_loom.select()
                        .where(upstream_generate_loom.c.id == int(data["generate_loom_id"]))
                      )
                row1 = await res1.fetchone()
                loom_file = row1['result_path'] + '/cellranger.loom'
                load_data_r_code = rbaseName+' <- read.loom.matrices(file = "'+loom_file+'")\n'+\
                    rbaseName+' <- as.Seurat(x = '+rbaseName+')\n'+\
                    rbaseName+'[["RNA"]] <- '+rbaseName+'[["spliced"]]\n'+\
                    'DefaultAssay('+rbaseName+') <- "RNA"\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

        elif (str(data["is_combined"]) != "Y" and str(data["data_type"]) == "fastq"
                and str(data["is_reads_mapping"]) == "Y" and str(data["is_generate_loom"]) == "-"
             ):
            async with request.app['mysql_db'].acquire() as conn:
                res = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
                      )
                row = await res.fetchone()
                qc_r_code = row["r_code"]

                res1 = await conn.execute(upstream_reads_mapping.select()
                        .where(upstream_reads_mapping.c.id == int(data["reads_mapping_id"]))
                      )
                row1 = await res1.fetchone()
                load_data_r_code = rbaseName+'.data <- Read10X("'+str(row1["result_path"])+'");'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200);'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-");'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]");'

        elif (str(data["is_combined"]) != "Y" and str(data["data_type"]) == "cellranger"
                and str(data["is_reads_mapping"]) == "-" and str(data["is_generate_loom"]) == "-"
             ):
            async with request.app['mysql_db'].acquire() as conn:
                res = await conn.execute(upload_data.select()
                    .where(upload_data.c.id == int(data["data_id"]))
                )
                row = await res.fetchone()

                res1 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
                      )
                row1 = await res1.fetchone()
                qc_r_code = row1["r_code"]

                load_data_r_code = rbaseName+'.data <- Read10X("'+str(row["upload_path"])+'");'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200);'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-");'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")'

        elif (str(data["is_combined"]) != "Y" and str(data["data_type"]) == "matrix"
                and str(data["is_reads_mapping"]) == "-" and str(data["is_generate_loom"]) == "-"
             ):
            async with request.app['mysql_db'].acquire() as conn:
                res = await conn.execute(upload_data.select()
                    .where(upload_data.c.id == int(data["data_id"]))
                )
                row = await res.fetchone()

                res1 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
                      )
                row1 = await res1.fetchone()
                qc_r_code = row1["r_code"]

                load_data_r_code = rbaseName+'.data <- read.table(file="'+row["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1);'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200);'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-");'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")'                                
        else:
            pass            
 
        # load R script header code
        await asyncio.to_thread(con.voidEval, 'setwd("'+str(config["tempImageDir"])+'")')
        await asyncio.to_thread(con.voidEval, 'library(Seurat)')
        await asyncio.to_thread(con.voidEval, 'library(SeuratObject)')
        await asyncio.to_thread(con.voidEval, 'library(ggplot2)')
        await asyncio.to_thread(con.voidEval, 'library(cowplot)')
        await asyncio.to_thread(con.voidEval, "library(SeuratWrappers)")
        await asyncio.to_thread(con.voidEval, "library(SeuratDisk)")
        await asyncio.to_thread(con.voidEval, read_loom_r_code)

        # load data & qc code
        await asyncio.to_thread(con.voidEval, load_data_r_code)
        await asyncio.to_thread(con.voidEval, qc_r_code)
        if str(data["is_combined"]) == "Y":
            await asyncio.to_thread(con.voidEval, combined_r_code)

        #
        if str(data["is_combined"]) == "Y" and str(data["combined_type"]) == "integrate":
            await asyncio.to_thread(con.voidEval, 'temp <- project.combined')
            await asyncio.to_thread(con.voidEval, 'temp <- RunPCA(project.combined, verbose = FALSE)')
        elif str(data["is_combined"]) == "Y" and str(data["combined_type"]) == "merge":
            await asyncio.to_thread(con.voidEval, 'temp <- project.combined')
            await asyncio.to_thread(con.voidEval, 'temp <- NormalizeData(temp)')
            await asyncio.to_thread(con.voidEval, 'temp <- FindVariableFeatures(temp)')
            await asyncio.to_thread(con.voidEval, 'temp <- ScaleData(temp, features = rownames(x = temp))')
            await asyncio.to_thread(con.voidEval, 'temp <- RunPCA(temp, verbose = FALSE)')
        else:
            await asyncio.to_thread(con.voidEval, 'temp <- '+rbaseName)
            await asyncio.to_thread(con.voidEval, 'temp <- NormalizeData(temp)')
            await asyncio.to_thread(con.voidEval, 'temp <- FindVariableFeatures(temp)')
            await asyncio.to_thread(con.voidEval, 'temp <- ScaleData(temp, features = rownames(x = temp))')
            await asyncio.to_thread(con.voidEval, 'temp <- RunPCA(temp, verbose = FALSE)')

    else:
        con = rserv_connections[sKey]
    
    # 2. output dc images
    elbow_png = "elbow_dc_" + str(uuid.uuid1()) + ".png"
    dim_png = "dim_dc_" + str(uuid.uuid1()) + ".png"

    await asyncio.to_thread(con.voidEval, 'p <- ElbowPlot(temp, ndims = 50)')
    await asyncio.to_thread(con.voidEval, 'temp <- RunUMAP(temp, reduction = "pca", dims = 1:'+data["dimension"]+')')
    await asyncio.to_thread(con.voidEval, 'temp <- FindNeighbors(temp, reduction = "pca", dims = 1:'+data["dimension"]+')')
    await asyncio.to_thread(con.voidEval, 'temp <- FindClusters(temp, resolution = '+data["resolution"]+')')
    await asyncio.to_thread(con.voidEval, 'ggsave("'+elbow_png+'", p, width=10, height=6, units="in")')

    # merge or integrate project output three images
    if str(data["is_combined"]) == "Y":
        await asyncio.to_thread(con.voidEval, 'p1 <- DimPlot(temp, reduction = "umap", group.by = "type")')
        await asyncio.to_thread(con.voidEval, 'p2 <- DimPlot(temp, reduction = "umap", label = TRUE, repel = TRUE) + NoLegend()')
        await asyncio.to_thread(con.voidEval, 'ggsave("'+dim_png+'", p1, width=10, height=6, units="in")')
        dim2_png = "dim2_dc_" + str(uuid.uuid1()) + ".png"
        await asyncio.to_thread(con.voidEval, 'ggsave("'+dim2_png+'", p2, width=10, height=6, units="in")')

        result = [{'id':0, 'url': "/tmpImg/"+elbow_png},{'id':1, 'url': "/tmpImg/"+dim_png},{'id':2, 'url': "/tmpImg/"+dim2_png}]
    else:
        await asyncio.to_thread(con.voidEval, 'p1 <- DimPlot(temp, reduction = "umap", label = TRUE, repel = TRUE) + NoLegend()')
        await asyncio.to_thread(con.voidEval, 'ggsave("'+dim_png+'", p1, width=10, height=6, units="in")')
        result = [{'id':0, 'url': "/tmpImg/"+elbow_png},{'id':1, 'url': "/tmpImg/"+dim_png}]

    return web.json_response(result)

async def scRNA_dc(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    rbaseName = '' # R variable name
    if data["is_combined"] == "Y":
        rbaseName = "project.combined"
        sKey = str(data["user_id"]) + ':' + 'dc:combine'+ ':'+ str(data["combined_id"])
    else:
        rbaseName = 'seurat_'+str(data["data_id"])
        sKey = str(data["user_id"]) + ':' + 'dc'+ ':'+ str(data["data_id"])

    # get number of seurat clusters
    async with lock_rserv:
        con = rserv_connections[sKey]
    ncluster = await asyncio.to_thread(con.eval, 'length(levels(temp$seurat_clusters))')
    ncluster = int(ncluster)
    # write result image
    base_dir = pathlib.Path(config["dataDir"]) / "upstream" / "seurat"
    userPath = base_dir / "_".join(["user", str(data["user_id"]), 
                                        "".join(random.sample(string.ascii_letters + string.digits, 5))])
    while userPath.exists():
        userPath = base_dir / "_".join(["user", str(data["user_id"]), 
                                        "".join(random.sample(string.ascii_letters + string.digits, 5))])
    await asyncio.to_thread(userPath.mkdir, parents=True)
    
    await asyncio.to_thread(con.voidEval, 'setwd("'+userPath.absolute().__str__()+'")')
    await asyncio.to_thread(con.voidEval, 'ggsave("elbow.png", p, width=10, height=6, units="in")')
    await asyncio.to_thread(con.voidEval, 'ggsave("dim.png", p1, width=10, height=6, units="in")')
    if data["is_combined"] == "Y":
        await asyncio.to_thread(con.voidEval, 'ggsave("dim2.png", p2, width=10, height=6, units="in")')
    # genereate marker file
    await asyncio.to_thread(con.voidEval, 'markers <- FindAllMarkers(temp, only.pos = TRUE, min.pct = 0.25, logfc.threshold = 0.25)')
    await asyncio.to_thread(con.voidEval, 'write.table(markers, file = "all_markers.xls", sep = "\t", row.names = F)')

    # close Rserv connection
    con.close()
    async with lock_rserv:
        del rserv_connections[sKey]
    
    code_result = ''
    if data["is_combined"] != "Y":
        code_result = rbaseName+' <- NormalizeData('+rbaseName+');'+\
        rbaseName+' <- FindVariableFeatures('+rbaseName+');'+\
        rbaseName+' <- ScaleData('+rbaseName+', features = rownames(x = '+rbaseName+'));'+\
        rbaseName+' <- RunPCA('+rbaseName+', verbose = FALSE);'+\
        rbaseName+' <- RunUMAP('+rbaseName+', dims = 1:'+str(data["dimension"])+');' + \
        rbaseName+' <- RunTSNE('+rbaseName+', dims = 1:'+data["dimension"]+');' + \
        rbaseName+' <- FindNeighbors('+rbaseName+', reduction = "pca", dims = 1:'+str(data["dimension"])+');' + \
        rbaseName+' <- FindClusters('+rbaseName+', resolution = '+str(data["resolution"])+');'
    else:
        if data["combined_type"] != "merge":
            code_result = 'project.combined <- RunPCA(project.combined, verbose = FALSE);'+\
            'project.combined <- RunUMAP(project.combined, dims = 1:'+str(data["dimension"])+');' + \
            'project.combined <- RunTSNE(project.combined, dims = 1:'+str(data["dimension"])+');' + \
            'project.combined <- FindNeighbors(project.combined, reduction = "pca", dims = 1:'+str(data["dimension"])+');' + \
            'project.combined <- FindClusters(project.combined, resolution = '+str(data["resolution"])+');'
        else: # combined_type == merge
            code_result = 'project.combined <- NormalizeData(project.combined);'+\
            'project.combined <- FindVariableFeatures(project.combined);'+\
            'project.combined <- ScaleData(project.combined, features = rownames(x = project.combined));'+\
            'project.combined <- RunPCA(project.combined, verbose = FALSE);'+\
            'project.combined <- RunUMAP(project.combined, dims = 1:'+str(data["dimension"])+');' + \
            'project.combined <- RunTSNE(project.combined, dims = 1:'+str(data["dimension"])+');' + \
            'project.combined <- FindNeighbors(project.combined, reduction = "pca", dims = 1:'+str(data["dimension"])+');' + \
            'project.combined <- FindClusters(project.combined, resolution = '+str(data["resolution"])+');'
    
    if data["is_combined"] != "Y":
        data_id = int(data["quality_control_id"])
        data_type = 'upstream_quality_control'
    else:
        data_id = int(data["combined_id"])
        if str(data["combined_type"]) == "merge":
            data_type = "upstream_merge"
        else:
            data_type = "upstream_integrate"

    async with request.app['mysql_db'].acquire() as conn:
        res = await conn.execute(upstream_dc.select()
                .where(upstream_dc.c.input_data_id == data_id)
                .where(upstream_dc.c.input_data_type == data_type)
            )
        row = await res.fetchone()
        if row:
            # 删除原本的分群结果目录
            rmtree(str(row["result_path"]))
            # 更新数据库中的分群结果
            async with conn.begin() as transaction:
                await conn.execute(upstream_dc.update()
                    .where(upstream_dc.c.input_data_id == data_id)
                    .where(upstream_dc.c.input_data_type == data_type) 
                    .values(
                        r_code = code_result,
                        n_cluster = ncluster,
                        result_path = userPath.absolute().__str__()
                ))
                await transaction.commit()

        else:
            async with conn.begin() as transaction:
                await conn.execute(upstream_dc.insert().values(
                        input_data_id = data_id,
                        input_data_type = data_type,
                        r_code = code_result,
                        n_cluster = ncluster,
                        result_path = userPath.absolute().__str__()
                    ))
                await transaction.commit()

        return web.Response(text="OK")

async def query_annotation_type_data(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)

    async with request.app['mysql_db'].acquire() as conn:
        results = []
        # 1. merge  -> dc
        # 2. integrate  -> dc
        # 3. fastq -> reads mapping -> generate_loom -> qc -> dc
        # 4. fastq -> reads mapping -> qc -> dc
        # 5. matrix -> qc -> dc
        # 6. cellranger -> qc -> dc

        # 1. merge data
        sql = '''
            SELECT a.id, a.input_data_ids, b.id, b.n_cluster
            FROM upstream_merge a, upstream_dc b
            WHERE a.id = b.input_data_id
                  AND b.input_data_type = 'upstream_merge'
                  AND a.user_id = $user_id;
        '''
        sql = sql.replace("$user_id", str(data["user_id"]))
        res = await conn.execute(sql)
        async for row in res:
                results.append({
                    "data_id": "-", 
                    "user_id": "-",
                    "upload_time": "-",
                    "sequencing_type": "-",
                    "data_type": "-",
                    "data_note": "-",
                    "species": "-",
                    "tissue": "-" ,
                    "is_reads_mapping": "-",
                    "reads_mapping_id": "-",
                    "is_generate_loom": "-",
                    "generate_loom_id": "-",
                    "is_quality_control": "-",
                    "quality_control_id": "-",
                    "is_combined": "Y",
                    "combined_id": str(row[0]),
                    "combined_type": "merge",
                    "combined_data_id": row[1],
                    "is_dc": "Y",
                    "dc_id": str(row[2]),
                    "n_cluster": str(row[3])
                })

        # 2. integrated data
        sql1 = '''
            SELECT a.id, a.input_data_ids, b.id, b.n_cluster
            FROM upstream_integrate a, upstream_dc b
            WHERE a.id = b.input_data_id 
                  AND b.input_data_type = 'upstream_integrate'
                  AND a.user_id = $user_id;
        '''
        sql1 = sql1.replace("$user_id", str(data["user_id"]))
        res1 = await conn.execute(sql1)
        async for row1 in res1:
                results.append({
                    "data_id": "-", 
                    "user_id": "-",
                    "upload_time": "-",
                    "sequencing_type": "-",
                    "data_type": "-",
                    "data_note": "-",
                    "species": "-",
                    "tissue": "-" ,
                    "is_reads_mapping": "-",
                    "reads_mapping_id": "-",
                    "is_generate_loom": "-",
                    "generate_loom_id": "-",
                    "is_quality_control": "-",
                    "quality_control_id": "-",
                    "is_combined": "Y",
                    "combined_id": str(row1[0]),
                    "combined_type": "integrate",
                    "combined_data_id": str(row1[1]),
                    "is_dc": "Y",
                    "dc_id": str(row1[2]),
                    "n_cluster": str(row1[3])
                })
            
        # 3. fastq -> reads mapping -> generate_loom -> qc -> dc
        sql2 = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.species, a.tissue, a.data_note, b.id, c.id, d.id, e.id, e.n_cluster
            FROM upload_data a, upstream_reads_mapping b, 
                 upstream_generate_loom c, upstream_quality_control d,
                 upstream_dc e
            WHERE a.id = b.upload_data_id 
                  AND b.id = c.upstream_reads_mapping_id 
                  AND c.id = d.input_data_id 
                  AND d.input_data_type = 'upstream_generate_loom'
                  AND d.id = e.input_data_id
                  AND e.input_data_type = 'upstream_quality_control'
                  AND a.user_id = $user_id;
        '''
        sql2 = sql2.replace("$user_id", str(data["user_id"]))
        res2 = await conn.execute(sql2)
        async for row2 in res2:
                results.append({
                    "data_id": str(row2[0]), 
                    "user_id": str(row2[1]),
                    "upload_time": str(row2[2]),
                    "sequencing_type": row2[3],
                    "data_type": row2[4],
                    "data_note": row2[5],
                    "species": row2[6],
                    "tissue": row2[7] ,
                    "is_reads_mapping": "Y",
                    "reads_mapping_id": str(row2[8]),
                    "is_generate_loom": "Y",
                    "generate_loom_id": str(row2[9]),
                    "is_quality_control": "Y",
                    "quality_control_id": str(row2[10]),
                    "is_combined": "-",
                    "combined_id": "-",
                    "combined_type": "-",
                    "combined_data_id": "-",
                    "is_dc": "Y",
                    "dc_id": str(row2[11]),
                    "n_cluster": str(row2[12])
                })
            
        # 4. fastq -> reads mapping -> qc -> dc
        sql3 = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.species, a.tissue, a.data_note, b.id, c.id, d.id, d.n_cluster
            FROM upload_data a, upstream_reads_mapping b,
                 upstream_quality_control c, upstream_dc d
            WHERE a.id = b.upload_data_id
                  AND b.id = c.input_data_id
                  AND c.input_data_type = 'upstream_reads_mapping'
                  AND c.id = d.input_data_id
                  AND d.input_data_type = 'upstream_quality_control'
                  AND a.user_id = $user_id;
        '''
        sql3 = sql3.replace("$user_id", str(data["user_id"]))
        res3 = await conn.execute(sql3)
        async for row3 in res3:
                results.append({
                    "data_id": str(row3[0]), 
                    "user_id": str(row3[1]),
                    "upload_time": str(row3[2]),
                    "sequencing_type": row3[3],
                    "data_type": row3[4],
                    "data_note": row3[5],
                    "species": row3[6],
                    "tissue": row3[7] ,
                    "is_reads_mapping": "Y",
                    "reads_mapping_id": str(row3[8]),
                    "is_generate_loom": "-",
                    "generate_loom_id": "-",
                    "is_quality_control": "Y",
                    "quality_control_id": str(row3[9]),
                    "is_combined": "-",
                    "combined_id": "-",
                    "combined_type": "-",
                    "combined_data_id": "-",
                    "is_dc": "Y",
                    "dc_id": str(row3[10]),
                    "n_cluster": str(row3[11])
                })

        # 5. matrix -> qc -> dc
        sql4 = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.species, a.tissue, a.data_note, b.id, c.id, c.n_cluster
            FROM upload_data a, upstream_quality_control b, upstream_dc c
            WHERE a.id = b.input_data_id 
                  AND b.id = c.input_data_id 
                  AND b.input_data_type = 'upload_data_matrix'
                  AND c.input_data_type = 'upstream_quality_control'
                  AND a.user_id = $user_id;
        '''
        sql4 = sql4.replace("$user_id", str(data["user_id"]))        
        res4 = await conn.execute(sql4)
        async for row4 in res4:
                results.append({
                    "data_id": str(row4[0]), 
                    "user_id": str(row4[1]),
                    "upload_time": str(row4[2]),
                    "sequencing_type": row4[3],
                    "data_type": row4[4],
                    "data_note": row4[5],
                    "species": row4[6],
                    "tissue": row4[7] ,
                    "is_reads_mapping": "-",
                    "reads_mapping_id": "-",
                    "is_generate_loom": "-",
                    "generate_loom_id": "-",
                    "is_quality_control": "Y",
                    "quality_control_id": str(row4[8]),
                    "is_combined": "-",
                    "combined_id": "-",
                    "combined_type": "-",
                    "combined_data_id": "-",
                    "is_dc": "Y",
                    "dc_id": str(row4[9]),
                    "n_cluster": str(row4[10])
                })
            
        # 6. cellranger -> qc -> dc
        sql5 = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.species, a.tissue, a.data_note, b.id, c.id, c.n_cluster
            FROM upload_data a, upstream_quality_control b, upstream_dc c
            WHERE a.id = b.input_data_id
                  AND b.id = c.input_data_id 
                  AND b.input_data_type = 'upload_data_cellranger'
                  AND c.input_data_type = 'upstream_quality_control'
                  AND a.user_id = $user_id;
        '''
        sql5 = sql5.replace("$user_id", str(data["user_id"]))
        res5 = await conn.execute(sql5)
        async for row5 in res5:
                results.append({
                    "data_id": str(row5[0]), 
                    "user_id": str(row5[1]),
                    "upload_time": str(row5[2]),
                    "sequencing_type": row5[3],
                    "data_type": row5[4],
                    "data_note": row5[5],
                    "species": row5[6],
                    "tissue": row5[7] ,
                    "is_reads_mapping": "-",
                    "reads_mapping_id": "-",
                    "is_generate_loom": "-",
                    "generate_loom_id": "-",
                    "is_quality_control": "Y",
                    "quality_control_id": str(row5[8]),
                    "is_combined": "-",
                    "combined_id": "-",
                    "combined_type": "-",
                    "combined_data_id": "-",
                    "is_dc": "Y",
                    "dc_id": str(row5[9]),
                    "n_cluster": str(row5[10])
                })
            
        return web.json_response(results)

async def download_type_marker_file(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)

    if data["is_combined"] != "Y":
        data_id = int(data["quality_control_id"])
        data_type = 'upstream_quality_control'
    else:
        data_id = int(data["combined_id"])
        if str(data["combined_type"]) == "merge":
            data_type = "upstream_merge"
        else:
            data_type = "upstream_integrate"

    async with request.app['mysql_db'].acquire() as conn:
        res = await conn.execute(upstream_dc.select()
                .where((upstream_dc.c.input_data_id == data_id) &
                       (upstream_dc.c.input_data_type == data_type)
                       )
            )
        row = await res.fetchone()

        filename = 'all_markers.xls'
        filepath = str(row["result_path"]) + "/" + filename
        async with aiofiles.open(filepath, 'rb') as f:
            file_data = await f.read()
        headers = {
            'Content-Disposition': f'attachment; filename="{filename}"',
            'Content-Type': 'application/vnd.ms-excel',
        }
        return web.Response(body=file_data, headers=headers)

async def show_scRNA_annotation_type_image(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    rbaseName = '' # R variable name
    if data["is_combined"] == "Y":
        rbaseName = "project.combined"
        sKey = str(data["user_id"]) + ':' + 'type'+ ':'+ str(data["combined_id"])
    else:
        rbaseName = 'seurat_'+str(data["data_id"])
        sKey = str(data["user_id"]) + ':' + 'type'+ ':'+ str(data["data_id"])
    
    if sKey not in rserv_connections: 
        con = await asyncio.to_thread(pyRserve.connect)
        async with lock_rserv:
            rserv_connections[sKey] = con

        # load data r code
        # query qc r code
        load_data_r_code = ''
        combined_r_code = ''
        qc_r_code = ''
        dc_r_code = ''

        if  str(data["is_combined"]) == "Y" and str(data["combined_type"]) == "merge":
            async with request.app['mysql_db'].acquire() as conn:
                res0 = await conn.execute(upstream_dc.select()
                    .where(upstream_dc.c.id == int(data["dc_id"]))
                )
                row0 = await res0.fetchone()
                dc_r_code = row0["r_code"]

                res = await conn.execute(upstream_merge.select()
                        .where(upstream_merge.c.id == int(data["combined_id"]))
                      )
                row = await res.fetchone()
                combined_r_code = row["r_code"]
            
                for x in str(row["input_qc_ids"]).split(':'):
                    res1 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.id == int(x))
                    )
                    row1 = await res1.fetchone()
                    qc_r_code += row1["r_code"] + ";"
                
                for i in str(row["input_data_ids"]).split(':'):
                    res2 = await conn.execute(upload_data.select()
                        .where(upload_data.c.id == int(i))
                    )
                    row2 = await res2.fetchone()

                    if str(row2["data_type"]) == "cellranger":
                        load_data_r_code += 'seurat_'+i+'.data <- Read10X("'+str(row2["upload_path"])+'");'+\
                        'seurat_'+i+' <- CreateSeuratObject(counts=seurat_'+i+'.data,project = "seurat_'+i+'",min.cells=3,min.features=200);'+\
                        'seurat_'+i+'[["percent.mt"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^MT-");'+\
                        'seurat_'+i+'[["percent.rb"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^RP[SL]");'

                    elif str(row2["data_type"]) == "fastq":
                        res3 = await conn.execute(upstream_reads_mapping.select()
                                .where(upstream_reads_mapping.c.id == int(data["reads_mapping_id"]))
                            )
                        row3 = await res3.fetchone()
                        load_data_r_code += 'seurat_'+i+'.data <- Read10X("'+str(row3["result_path"])+'");'+\
                        'seurat_'+i+' <- CreateSeuratObject(counts=seurat_'+i+'.data,project = "seurat_'+i+'",min.cells=3,min.features=200);'+\
                        'seurat_'+i+'[["percent.mt"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^MT-");'+\
                        'seurat_'+i+'[["percent.rb"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^RP[SL]");'                        
                    else:
                        pass

        elif str(data["is_combined"]) == "Y" and str(data["combined_type"]) == "integrate":
            async with request.app['mysql_db'].acquire() as conn:
                res0 = await conn.execute(upstream_dc.select()
                    .where(upstream_dc.c.id == int(data["dc_id"]))
                )
                row0 = await res0.fetchone()
                dc_r_code = row0["r_code"]

                res = await conn.execute(upstream_integrate.select()
                    .where(upstream_integrate.c.id == int(data["combined_id"]))
                )
                row = await res.fetchone()
                combined_r_code = row["r_code"]
                                
                for x in str(row["input_qc_ids"]).split(':'):
                    res1 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.id == int(x))
                    )
                    row1 = await res1.fetchone()
                    qc_r_code += row1["r_code"] + ";"
                
                for i in str(row["input_data_ids"]).split(':'):
                    res2 = await conn.execute(upload_data.select()
                        .where(upload_data.c.id == int(i))
                    )
                    row2 = await res2.fetchone()

                    if str(row2["data_type"]) == "cellranger":
                        load_data_r_code += 'seurat_'+i+'.data <- Read10X("'+str(row2["upload_path"])+'");'+\
                        'seurat_'+i+' <- CreateSeuratObject(counts=seurat_'+i+'.data,project = "seurat_'+i+'",min.cells=3,min.features=200);'+\
                        'seurat_'+i+'[["percent.mt"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^MT-");'+\
                        'seurat_'+i+'[["percent.rb"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^RP[SL]");'

                    elif str(row2["data_type"]) == "fastq":
                        res3 = await conn.execute(upstream_reads_mapping.select()
                                .where(upstream_reads_mapping.c.id == int(data["reads_mapping_id"]))
                            )
                        row3 = await res3.fetchone()
                        load_data_r_code += 'seurat_'+i+'.data <- Read10X("'+str(row3["result_path"])+'");'+\
                        'seurat_'+i+' <- CreateSeuratObject(counts=seurat_'+i+'.data,project = "seurat_'+i+'",min.cells=3,min.features=200);'+\
                        'seurat_'+i+'[["percent.mt"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^MT-");'+\
                        'seurat_'+i+'[["percent.rb"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^RP[SL]");'                        
                    else:
                        pass

        elif (str(data["is_combined"]) != "Y" and str(data["data_type"]) == "fastq"
                and str(data["is_reads_mapping"]) == "Y" and str(data["is_generate_loom"]) == "Y"
             ):    
            async with request.app['mysql_db'].acquire() as conn:
                res0 = await conn.execute(upstream_dc.select()
                    .where(upstream_dc.c.id == int(data["dc_id"]))
                )
                row0 = await res0.fetchone()
                dc_r_code = row0["r_code"]                

                res = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
                      )
                row = await res.fetchone()
                qc_r_code = row["r_code"]

                res1 = await conn.execute(upstream_generate_loom.select()
                        .where(upstream_generate_loom.c.id == int(data["generate_loom_id"]))
                      )
                row1 = await res1.fetchone()
                loom_file = row1['result_path'] + '/cellranger.loom'
                load_data_r_code = rbaseName+' <- read.loom.matrices(file = "'+loom_file+'");'+\
                    rbaseName+' <- as.Seurat('+rbaseName+');'+\
                    rbaseName+'[["RNA"]] <- '+rbaseName+'[["spliced"]];'+\
                    'DefaultAssay('+rbaseName+') <- "RNA";'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-");'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]");'

        elif (str(data["is_combined"]) != "Y" and str(data["data_type"]) == "fastq"
                and str(data["is_reads_mapping"]) == "Y" and str(data["is_generate_loom"]) == "-"
             ):
            async with request.app['mysql_db'].acquire() as conn:
                res0 = await conn.execute(upstream_dc.select()
                    .where(upstream_dc.c.id == int(data["dc_id"]))
                )
                row0 = await res0.fetchone()
                dc_r_code = row0["r_code"]

                res = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
                      )
                row = await res.fetchone()
                qc_r_code = row["r_code"]

                res1 = await conn.execute(upstream_reads_mapping.select()
                        .where(upstream_reads_mapping.c.id == int(data["reads_mapping_id"]))
                      )
                row1 = await res1.fetchone()
                load_data_r_code = rbaseName+'.data <- Read10X("'+str(row1["result_path"])+'");'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200);'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-");'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]");'

        elif (str(data["is_combined"]) != "Y" and str(data["data_type"]) == "cellranger"
                and str(data["is_reads_mapping"]) == "-" and str(data["is_generate_loom"]) == "-"
             ):
            async with request.app['mysql_db'].acquire() as conn:
                res0 = await conn.execute(upstream_dc.select()
                    .where(upstream_dc.c.id == int(data["dc_id"]))
                )
                row0 = await res0.fetchone()
                dc_r_code = row0["r_code"]

                res = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
                      )
                row = await res.fetchone()
                qc_r_code = row["r_code"]

                res2 = await conn.execute(upload_data.select()
                        .where(upload_data.c.id == int(data["data_id"]))
                    )
                row2 = await res2.fetchone()

                load_data_r_code = rbaseName+'.data <- Read10X("'+str(row2["upload_path"])+'");'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200);'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-");'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]");'

        elif (str(data["is_combined"]) != "Y" and str(data["data_type"]) == "matrix"
                and str(data["is_reads_mapping"]) == "-" and str(data["is_generate_loom"]) == "-"
             ):                    
            async with request.app['mysql_db'].acquire() as conn:
                res0 = await conn.execute(upstream_dc.select()
                    .where(upstream_dc.c.id == int(data["dc_id"]))
                )
                row0 = await res0.fetchone()
                dc_r_code = row0["r_code"]

                res = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
                      )
                row = await res.fetchone()
                qc_r_code = row["r_code"]

                res2 = await conn.execute(upload_data.select()
                        .where(upload_data.c.id == int(data["data_id"]))
                    )
                row2 = await res2.fetchone()

                load_data_r_code = rbaseName+'.data <- read.table(file="'+row2["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
        else:
            pass            
        
        # load R script header code
        await asyncio.to_thread(con.voidEval, 'setwd("'+str(config["tempImageDir"])+'")')
        await asyncio.to_thread(con.voidEval, 'library(Seurat)')
        await asyncio.to_thread(con.voidEval, 'library(SeuratObject)')
        await asyncio.to_thread(con.voidEval, 'library(ggplot2)')
        await asyncio.to_thread(con.voidEval, 'library(cowplot)')
        await asyncio.to_thread(con.voidEval, "library(SeuratWrappers)")
        await asyncio.to_thread(con.voidEval, "library(SeuratDisk)")
        await asyncio.to_thread(con.voidEval, read_loom_r_code)

        # load data & qc code & dc code
        await asyncio.to_thread(con.voidEval, load_data_r_code)
        await asyncio.to_thread(con.voidEval, qc_r_code)
        if str(data["is_combined"]) == 'Y':
            await asyncio.to_thread(con.voidEval, combined_r_code)
        await asyncio.to_thread(con.voidEval, dc_r_code)

        # dimplot
        await asyncio.to_thread(con.voidEval, 
            'p <- DimPlot('+rbaseName+', reduction = "umap", label = TRUE) + NoLegend()')

    else:
        async with lock_rserv:
            con = rserv_connections[sKey]
    
    # 2. output annotation_type images
    feature_png = "feature_type_" + str(uuid.uuid1()) + ".png"
    dim_png = "dim_type_" + str(uuid.uuid1()) + ".png"
    violin_png = "violin_type_" + str(uuid.uuid1()) + ".png"

    await asyncio.to_thread(con.voidEval, 'ggsave("'+dim_png+'", p, width=10, height=6, units="in")')
    await asyncio.to_thread(con.voidEval, 'p1 <- VlnPlot('+rbaseName+', features ="'+str(data["marker"])+'", pt.size = 0, group.by = "seurat_clusters")')
    await asyncio.to_thread(con.voidEval, 'ggsave("'+violin_png+'", p1, width=10, height=6, units="in")')
    await asyncio.to_thread(con.voidEval, 'p2 <- FeaturePlot('+rbaseName+', features ="'+str(data["marker"])+'")')
    await asyncio.to_thread(con.voidEval, 'ggsave("'+feature_png+'", p2, width=10, height=6, units="in")')
    # show type dimplot
    if str(data["is_combined"]) == 'Y':
        dim_type_png = "dim_type2_" + str(uuid.uuid1()) + ".png"
        await asyncio.to_thread(con.voidEval,
            'p3 <- DimPlot('+rbaseName+', reduction = "umap", label = TRUE, group.by = "type")')
        await asyncio.to_thread(con.voidEval, 'ggsave("'+dim_type_png+'", p3, width=10, height=6, units="in")')
        results = [{'id':0, 'url': "/tmpImg/"+dim_png},{'id':1, 'url': "/tmpImg/"+feature_png},{'id':2, 'url': "/tmpImg/"+violin_png},{'id':3, 'url': "/tmpImg/"+dim_type_png}]
    else:
        results = [{'id':0, 'url': "/tmpImg/"+dim_png},{'id':1, 'url': "/tmpImg/"+feature_png},{'id':2, 'url': "/tmpImg/"+violin_png}]

    return web.json_response(results)

async def scRNA_annotation_type(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)

    rbaseName = '' # R variable name
    if data["is_combined"] == "Y":
        rbaseName = "project.combined"
        sKey = str(data["user_id"]) + ':' + 'type'+ ':'+ str(data["combined_id"])
    else:
        rbaseName = 'seurat_'+str(data["data_id"])
        sKey = str(data["user_id"]) + ':' + 'type'+ ':'+ str(data["data_id"])

    anno_r_code = rbaseName+'[["check_clusters"]] = ""\n'

    for index, anno in enumerate(str(data["anno"]).split(',')):
        anno_r_code += rbaseName+'$check_clusters['+rbaseName+'$seurat_clusters == '+str(index)+'] = "'+anno+'"\n'

    base_dir = pathlib.Path(config["dataDir"]) / "annotation" / "type"
    userPath = base_dir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    while userPath.exists():
        userPath = base_dir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    await asyncio.to_thread(userPath.mkdir, parents=True)

    cluster_order = '"'+str(data["cluster_name_order"]).replace(':', '","')+'"'
    marker_gene_order = '"'+str(data["marker_gene_order"]).replace(':', '","')+'"'

    async with lock_rserv:
        con = rserv_connections[sKey]

    await asyncio.to_thread(con.voidEval,'setwd("'+userPath.absolute().__str__()+'")')
    await asyncio.to_thread(con.voidEval, anno_r_code)
    await asyncio.to_thread(con.voidEval, 'Idents(object = '+rbaseName+') <- "check_clusters"')
    await asyncio.to_thread(con.voidEval, 'cluster_order <- c('+cluster_order+')')
    await asyncio.to_thread(con.voidEval, 'levels('+rbaseName+') <- cluster_order')
    await asyncio.to_thread(con.voidEval, 'ph <- '+seurat_DoHeatmap.replace("$marker_gene_order", marker_gene_order).replace("$rbaseName", rbaseName))
    await asyncio.to_thread(con.voidEval, 'pv <- '+seurat_VlnPlot.replace("$marker_gene_order", marker_gene_order).replace("$rbaseName", rbaseName))
    await asyncio.to_thread(con.voidEval, 'ggsave("marker_vln.png", pv, width=10, height=6, units="in")')
    await asyncio.to_thread(con.voidEval, 'ggsave("marker_heatmap.png", ph, width=10, height=6, units="in")')
    await asyncio.to_thread(con.voidEval, 'ggsave("dim1.png", p, width=10, height=6, units="in")')
    await asyncio.to_thread(con.voidEval, 'p3 <- DimPlot('+rbaseName+', reduction = "umap", label = T) + NoLegend()')
    await asyncio.to_thread(con.voidEval, 'ggsave("dim2.png", p3, width=10, height=6, units="in")')

    # close Rserv connection
    con.close()
    async with lock_rserv:
        del rserv_connections[sKey]

    async with request.app['mysql_db'].acquire() as conn:
        res = await conn.execute(annotation_type.select()
                .where(annotation_type.c.upstream_dc_id == int(data["dc_id"]))
            )
        row = await res.fetchone()
        if row:
            # 删除原本的分群结果目录
            rmtree(str(row["result_path"]))
            # 更新数据库中的分群结果
            async with conn.begin() as transaction:
                await conn.execute(annotation_type.update()
                    .where(annotation_type.c.upstream_dc_id == int(data["dc_id"]))
                    .values(
                        r_code = anno_r_code,
                        result_path = userPath.absolute().__str__(),
                        str_anno = str(data["anno"]),
                        cluster_name_order = str(data["cluster_name_order"]),
                        marker_gene_order = str(data["marker_gene_order"])
                ))
                await transaction.commit()
        else:
            async with conn.begin() as transaction:
                await conn.execute(annotation_type.insert().values(
                    user_id=int(data["user_id"]),
                    upstream_dc_id=int(data["dc_id"]),
                    r_code = anno_r_code,
                    str_anno = str(data["anno"]),
                    cluster_name_order = str(data["cluster_name_order"]),
                    marker_gene_order = str(data["marker_gene_order"]),
                    result_path = userPath.absolute().__str__()
                ))
                await transaction.commit()

        return web.Response(text="OK")

async def query_subtype_data(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)

    # 1. fastq -> reads mapping -> generate loom -> qc -> dc -> anno
    # 2. fastq -> reads mapping -> qc -> dc -> anno
    # 3. merge -> dc -> anno
    # 4. integrate -> dc -> anno
    # 5. cellrager -> qc -> dc -> anno
    # 6. matrix -> qc -> dc -> anno

    results = []
    async with request.app['mysql_db'].acquire() as conn:
        # 1. fastq -> reads mapping -> generate loom -> qc -> dc -> anno
        sql = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.species, a.tissue, a.data_note, b.id, c.id, d.id, e.id, f.id, f.cluster_name_order
            FROM upload_data a, upstream_reads_mapping b, 
                 upstream_generate_loom c, upstream_quality_control d,
                 upstream_dc e, annotation_type f
            WHERE a.id = b.upload_data_id
                  AND b.id = c.upstream_reads_mapping_id 
                  AND c.id = d.input_data_id 
                  AND d.id = e.input_data_id
                  AND d.input_data_type = 'upstream_generate_loom'
                  AND e.id = f.upstream_dc_id
                  AND e.input_data_type = 'upstream_quality_control'
                  AND a.user_id = $user_id;
        '''
        sql = sql.replace("$user_id", str(data['user_id']))
        res = await conn.execute(sql)
        async for row in res:
            results.append({
                "data_id": str(row[0]), 
                "user_id": str(row[1]),
                "upload_time": str(row[2]),
                "sequencing_type": row[3],
                "data_type": row[4],
                "data_note": row[5],
                "species": row[6],
                "tissue": row[7] ,
                "is_reads_mapping": "Y",
                "reads_mapping_id": str(row[8]),
                "is_generate_loom": "Y",
                "generate_loom_id": str(row[9]),
                "is_quality_control": "Y",
                "quality_control_id": str(row[10]),
                "is_combined": "-",
                "combined_id": "-",
                "combined_type": "-",
                "combined_data_ids": "-",
                "is_dc": "Y",
                "dc_id": str(row[11]),
                "is_anno_cluster": "Y",
                "anno_cluster_id": str(row[12]),
                "anno_cluster_result": row[13]
            })
        # 2. fastq -> reads mapping -> qc -> dc -> anno
        sql1 = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.species, a.tissue, a.data_note, b.id, c.id, d.id, e.id, e.cluster_name_order
            FROM upload_data a, upstream_reads_mapping b,
                 upstream_quality_control c, upstream_dc d,
                 annotation_type e
            WHERE a.id = b.upload_data_id 
                  AND b.id = c.input_data_id 
                  AND c.id = d.input_data_id 
                  AND c.input_data_type = 'upstream_reads_mapping'
                  AND d.input_data_type = 'upstream_quality_control'
                  AND d.id = e.upstream_dc_id 
                  AND a.user_id = $user_id;
        '''
        sql1 = sql1.replace("$user_id", str(data['user_id']))
        res1 = await conn.execute(sql1)
        async for row1 in res1:
            results.append({
                "data_id": str(row1[0]), 
                "user_id": str(row1[1]),
                "upload_time": str(row1[2]),
                "sequencing_type": row1[3],
                "data_type": row1[4],
                "data_note": row1[5],
                "species": row1[6],
                "tissue": row1[7] ,
                "is_reads_mapping": "Y",
                "reads_mapping_id": str(row1[8]),
                "is_generate_loom": "-",
                "generate_loom_id": "-",
                "is_quality_control": "Y",
                "quality_control_id": str(row1[9]),
                "is_combined": "-",
                "combined_id": "-",
                "combined_type": "-",
                "combined_data_ids": "-",
                "is_dc": "Y",
                "dc_id": str(row1[10]),
                "is_anno_cluster": "Y",
                "anno_cluster_id": str(row1[11]),
                "anno_cluster_result": row1[12]
            })
        # 3. merge -> dc -> anno
        sql2 = '''
            SELECT a.id, a.input_data_ids, b.id, c.id, c.cluster_name_order
            FROM upstream_merge a, upstream_dc b, annotation_type c
            WHERE a.id = b.input_data_id 
                  AND b.input_data_type = 'upstream_merge'
                  AND b.id = c.upstream_dc_id
                  AND a.user_id = $user_id;
        '''
        sql2 = sql2.replace("$user_id", str(data['user_id']))
        res2 = await conn.execute(sql2)
        async for row2 in res2:
            results.append({
                "data_id": "-", 
                "user_id": "-",
                "upload_time": "-",
                "sequencing_type": "-",
                "data_type": "-",
                "data_note": "-",
                "species": "-",
                "tissue": "-",
                "is_reads_mapping": "-",
                "reads_mapping_id": "-",
                "is_generate_loom": "-",
                "generate_loom_id": "-",
                "is_quality_control": "-",
                "quality_control_id": "-",
                "is_combined": "Y",
                "combined_id": str(row2[0]),
                "combined_type": "merge",
                "combined_data_ids": row2[1],
                "is_dc": "Y",
                "dc_id": str(row2[2]),
                "is_anno_cluster": "Y",
                "anno_cluster_id": str(row2[3]),
                "anno_cluster_result": row2[4]
            })

        # 4. integrate -> dc -> anno
        sql3 = '''
            SELECT a.id, a.input_data_ids, b.id, c.id, c.cluster_name_order
            FROM upstream_integrate a, upstream_dc b, annotation_type c
            WHERE a.id = b.input_data_id
                  AND b.input_data_type = 'upstream_integrate'
                  AND b.id = c.upstream_dc_id
                  AND a.user_id = $user_id;        
        '''
        sql3 = sql3.replace("$user_id", str(data['user_id']))
        res3 = await conn.execute(sql3)
        async for row3 in res3:
            results.append({
                "data_id": "-", 
                "user_id": "-",
                "upload_time": "-",
                "sequencing_type": "-",
                "data_type": "-",
                "data_note": "-",
                "species": "-",
                "tissue": "-",
                "is_reads_mapping": "-",
                "reads_mapping_id": "-",
                "is_generate_loom": "-",
                "generate_loom_id": "-",
                "is_quality_control": "-",
                "quality_control_id": "-",
                "is_combined": "Y",
                "combined_id": str(row3[0]),
                "combined_type": "integrate",
                "combined_data_ids": row3[1],
                "is_dc": "Y",
                "dc_id": str(row3[2]),
                "is_anno_cluster": "Y",
                "anno_cluster_id": str(row3[3]),
                "anno_cluster_result": row3[4]
            })
        
        # 5. cellranger -> qc -> dc -> anno
        sql4 = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.species, a.tissue, a.data_note, b.id, c.id, d.id, d.cluster_name_order
            FROM upload_data a, upstream_quality_control b, upstream_dc c, annotation_type d
            WHERE a.id = b.input_data_id 
                  AND b.id = c.input_data_id
                  AND b.input_data_type = 'upload_data_cellranger'
                  AND c.input_data_type = 'upstream_quality_control'
                  AND c.id = d.upstream_dc_id 
                  AND a.user_id = $user_id;
        '''
        sql4 = sql4.replace("$user_id", str(data["user_id"]))        
        res4 = await conn.execute(sql4)
        async for row4 in res4:
            results.append({
                "data_id": str(row4[0]), 
                "user_id": str(row4[1]),
                "upload_time": str(row4[2]),
                "sequencing_type": row4[3],
                "data_type": row4[4],
                "data_note": row4[5],
                "species": row4[6],
                "tissue": row4[7] ,
                "is_reads_mapping": "-",
                "reads_mapping_id": "-",
                "is_generate_loom": "-",
                "generate_loom_id": "-",
                "is_quality_control": "Y",
                "quality_control_id": str(row4[8]),
                "is_combined": "-",
                "combined_id": "-",
                "combined_type": "-",
                "combined_data_ids": "-",
                "is_dc": "Y",
                "dc_id": str(row4[9]),
                "is_anno_cluster": "Y",
                "anno_cluster_id": str(row4[10]),
                "anno_cluster_result": row4[11]
            })
        
        # 6. matrix -> qc -> dc 
        sql5 = '''
            SELECT a.id, a.user_id, a.upload_time, a.sequencing_type, a.data_type,
                   a.species, a.tissue, a.data_note, b.id, c.id, d.id, d.cluster_name_order
            FROM upload_data a, upstream_quality_control b, upstream_dc c, annotation_type d
            WHERE a.id = b.input_data_id 
                  AND b.id = c.input_data_id
                  AND b.input_data_type = 'upload_data_matrix'
                  AND c.id = d.upstream_dc_id
                  AND c.input_data_type = 'upstream_quality_control'
                  AND a.user_id = $user_id;
        '''
        sql5 = sql5.replace("$user_id", str(data["user_id"]))        
        res5 = await conn.execute(sql5)
        async for row5 in res5:
            results.append({
                "data_id": str(row5[0]), 
                "user_id": str(row5[1]),
                "upload_time": str(row5[2]),
                "sequencing_type": row5[3],
                "data_type": row5[4],
                "data_note": row5[5],
                "species": row5[6],
                "tissue": row5[7] ,
                "is_reads_mapping": "-",
                "reads_mapping_id": "-",
                "is_generate_loom": "-",
                "generate_loom_id": "-",
                "is_quality_control": "Y",
                "quality_control_id": str(row5[8]),
                "is_combined": "-",
                "combined_id": "-",
                "combined_type": "-",
                "combined_data_ids": "-",
                "is_dc": "Y",
                "dc_id": str(row5[9]),
                "is_anno_cluster": "Y",
                "anno_cluster_id": str(row5[10]),
                "anno_cluster_result": row5[11]
            })
        
        return web.json_response(results)

async def show_subtype_dc_image(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    rbaseName = '' # R variable name
    sKey = str(data["user_id"]) + ':' + str(data['anno_cluster_id'])+ ':'+ str(data["target"])
    if data["is_combined"] == "Y":
        rbaseName = "project.combined"
    else:
        rbaseName = 'seurat_'+str(data["data_id"])
        
    # 1. check if user has a Rserv connection
    if sKey not in rserv_connections:
        con = await asyncio.to_thread(pyRserve.connect)
        async with lock_rserv:
            rserv_connections[sKey] = con

        # load data r code
        # query qc r code
        load_data_r_code = ''
        combined_r_code = ''
        qc_r_code = ''
        dc_r_code = ''
        anno_r_code = ''

        if  str(data["is_combined"]) == "Y" and str(data["combined_type"]) == "merge":
            async with request.app['mysql_db'].acquire() as conn:
                res00 = await conn.execute(annotation_type.select()
                    .where(annotation_type.c.id == int(data["anno_cluster_id"]))
                )
                row00 = await res00.fetchone()
                anno_r_code = row00["r_code"]

                res0 = await conn.execute(upstream_dc.select()
                    .where(upstream_dc.c.id == int(data["dc_id"]))
                )
                row0 = await res0.fetchone()
                dc_r_code = row0["r_code"]

                res = await conn.execute(upstream_merge.select()
                        .where(upstream_merge.c.id == int(data["combined_id"]))
                      )
                row = await res.fetchone()
                combined_r_code = row["r_code"]
            
                for x in str(row["input_qc_ids"]).split(':'):
                    res1 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.id == int(x))
                    )
                    row1 = await res1.fetchone()
                    qc_r_code += row1["r_code"]+";"
                
                for i in str(row["input_data_ids"]).split(':'):
                    res2 = await conn.execute(upload_data.select()
                        .where(upload_data.c.id == int(i))
                    )
                    row2 = await res2.fetchone()

                    if str(row2["data_type"]) == "cellranger":
                        load_data_r_code += 'seurat_'+i+'.data <- Read10X("'+str(row2["upload_path"])+'")\n'+\
                        'seurat_'+i+' <- CreateSeuratObject(counts=seurat_'+i+'.data,project = "seurat_'+i+'",min.cells=3,min.features=200)\n'+\
                        'seurat_'+i+'[["percent.mt"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^MT-")\n'+\
                        'seurat_'+i+'[["percent.rb"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^RP[SL]")\n'

                    elif str(row2["data_type"]) == "fastq":
                        res3 = await conn.execute(upstream_reads_mapping.select()
                                .where(upstream_reads_mapping.c.id == int(data["reads_mapping_id"]))
                            )
                        row3 = await res3.fetchone()
                        load_data_r_code = 'seurat_'+i+'.data <- Read10X("'+str(row3["result_path"])+'")\n'+\
                        'seurat_'+i+' <- CreateSeuratObject(counts=seurat_'+i+'.data,project = "seurat_'+i+'",min.cells=3,min.features=200)\n'+\
                        'seurat_'+i+'[["percent.mt"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^MT-")\n'+\
                        'seurat_'+i+'[["percent.rb"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^RP[SL]")\n'                        
                    else:
                        pass

        elif str(data["is_combined"]) == "Y" and str(data["combined_type"]) == "integrate":
            async with request.app['mysql_db'].acquire() as conn:
                res00 = await conn.execute(annotation_type.select()
                    .where(annotation_type.c.id == int(data["anno_cluster_id"]))
                )
                row00 = await res00.fetchone()
                anno_r_code = row00["r_code"]

                res0 = await conn.execute(upstream_dc.select()
                    .where(upstream_dc.c.id == int(data["dc_id"]))
                )
                row0 = await res0.fetchone()
                dc_r_code = row0["r_code"]

                res = await conn.execute(upstream_merge.select()
                    .where(upstream_merge.c.id == int(data["combined_id"]))
                )
                row = await res.fetchone()
                combined_r_code = row["r_code"]
                                
                for x in str(row["input_qc_ids"]).split(':'):
                    res1 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.id == int(x))
                    )
                    row1 = await res1.fetchone()
                    qc_r_code += row1["r_code"]+";"
                
                for i in str(row["input_data_ids"]).split(':'):
                    res2 = await conn.execute(upload_data.select()
                        .where(upload_data.c.id == int(i))
                    )
                    row2 = await res2.fetchone()

                    if str(row2["data_type"]) == "cellranger":
                        load_data_r_code += 'seurat_'+i+'.data <- Read10X("'+str(row2["upload_path"])+'")\n'+\
                        'seurat_'+i+' <- CreateSeuratObject(counts=seurat_'+i+'.data,project = "seurat_'+i+'",min.cells=3,min.features=200)\n'+\
                        'seurat_'+i+'[["percent.mt"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^MT-")\n'+\
                        'seurat_'+i+'[["percent.rb"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^RP[SL]")\n'

                    elif str(row2["data_type"]) == "fastq":
                        res3 = await conn.execute(upstream_reads_mapping.select()
                                .where(upstream_reads_mapping.c.id == int(data["reads_mapping_id"]))
                            )
                        row3 = await res3.fetchone()
                        load_data_r_code = 'seurat_'+i+'.data <- Read10X("'+str(row3["result_path"])+'")\n'+\
                        'seurat_'+i+' <- CreateSeuratObject(counts=seurat_'+i+'.data,project = "seurat_'+i+'",min.cells=3,min.features=200)\n'+\
                        'seurat_'+i+'[["percent.mt"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^MT-")\n'+\
                        'seurat_'+i+'[["percent.rb"]] <- PercentageFeatureSet(seurat_'+i+', pattern = "^RP[SL]")\n'                        
                    else:
                        pass

        elif (str(data["is_combined"]) != "Y" and str(data["data_type"]) == "fastq"
                and str(data["is_reads_mapping"]) == "Y" and str(data["is_generate_loom"]) == "Y"
             ):    
            async with request.app['mysql_db'].acquire() as conn:
                res00 = await conn.execute(annotation_type.select()
                    .where(annotation_type.c.id == int(data["anno_cluster_id"]))
                )
                row00 = await res00.fetchone()
                anno_r_code = row00["r_code"]
                
                res0 = await conn.execute(upstream_dc.select()
                    .where(upstream_dc.c.id == int(data["dc_id"]))
                )
                row0 = await res0.fetchone()
                dc_r_code = row0["r_code"]                

                res = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
                      )
                row = await res.fetchone()
                qc_r_code = row["r_code"]

                res1 = await conn.execute(upstream_generate_loom.select()
                        .where(upstream_generate_loom.c.id == int(data["generate_loom_id"]))
                      )
                row1 = await res1.fetchone()
                loom_file = row1['result_path'] + '/cellranger.loom'
                load_data_r_code = rbaseName+' <- read.loom.matrices(file = "'+loom_file+'")\n'+\
                    rbaseName+' <- as.Seurat('+rbaseName+')\n'+\
                    rbaseName+'[["RNA"]] <- '+rbaseName+'[["spliced"]]\n'+\
                    'DefaultAssay('+rbaseName+') <- "RNA"\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

        elif (str(data["is_combined"]) != "Y" and str(data["data_type"]) == "fastq"
                and str(data["is_reads_mapping"]) == "Y" and str(data["is_generate_loom"]) == "-"
             ):
            async with request.app['mysql_db'].acquire() as conn:
                res00 = await conn.execute(annotation_type.select()
                    .where(annotation_type.c.id == int(data["anno_cluster_id"]))
                )
                row00 = await res00.fetchone()
                anno_r_code = row00["r_code"]

                res0 = await conn.execute(upstream_dc.select()
                    .where(upstream_dc.c.id == int(data["dc_id"]))
                )
                row0 = await res0.fetchone()
                dc_r_code = row0["r_code"]

                res = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
                      )
                row = await res.fetchone()
                qc_r_code = row["r_code"]

                res1 = await conn.execute(upstream_reads_mapping.select()
                        .where(upstream_reads_mapping.c.id == int(data["reads_mapping_id"]))
                      )
                row1 = await res1.fetchone()
                load_data_r_code = rbaseName+'.data <- Read10X("'+str(row1["result_path"])+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

        elif (str(data["is_combined"]) != "Y" and str(data["data_type"]) == "cellranger"
                and str(data["is_reads_mapping"]) == "-" and str(data["is_generate_loom"]) == "-"
             ):
            async with request.app['mysql_db'].acquire() as conn:
                res00 = await conn.execute(annotation_type.select()
                    .where(annotation_type.c.id == int(data["anno_cluster_id"]))
                )
                row00 = await res00.fetchone()
                anno_r_code = row00["r_code"]
                
                res0 = await conn.execute(upstream_dc.select()
                    .where(upstream_dc.c.id == int(data["dc_id"]))
                )
                row0 = await res0.fetchone()
                dc_r_code = row0["r_code"]

                res = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
                      )
                row = await res.fetchone()
                qc_r_code = row["r_code"]

                res1 = await conn.execute(upload_data.select()
                            .where(upload_data.c.id == int(data["data_id"]))
                        )
                row1 = await res1.fetchone()

                load_data_r_code = rbaseName+'.data <- Read10X("'+str(row1["upload_path"])+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

        elif (str(data["is_combined"]) != "Y" and str(data["data_type"]) == "matrix"
                and str(data["is_reads_mapping"]) == "-" and str(data["is_generate_loom"]) == "-"
             ):             
            async with request.app['mysql_db'].acquire() as conn:
                res00 = await conn.execute(annotation_type.select()
                    .where(annotation_type.c.id == int(data["anno_cluster_id"]))
                )
                row00 = await res00.fetchone()
                anno_r_code = row00["r_code"]
                
                res0 = await conn.execute(upstream_dc.select()
                    .where(upstream_dc.c.id == int(data["dc_id"]))
                )
                row0 = await res0.fetchone()
                dc_r_code = row0["r_code"]

                res = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
                      )
                row = await res.fetchone()
                qc_r_code = row["r_code"]

                res1 = await conn.execute(upload_data.select()
                        .where(upload_data.c.id == int(data["data_id"]))
                      )
                row1 = await res1.fetchone()

                load_data_r_code = rbaseName+'.data <- read.table(file="'+row1["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
        else:
            pass
         
        # load R script header code
        await asyncio.to_thread(con.voidEval, 'setwd("'+str(config["tempImageDir"])+'")')
        await asyncio.to_thread(con.voidEval, 'library(Seurat)')
        await asyncio.to_thread(con.voidEval, 'library(SeuratObject)')
        await asyncio.to_thread(con.voidEval, 'library(ggplot2)')
        await asyncio.to_thread(con.voidEval, 'library(cowplot)')
        await asyncio.to_thread(con.voidEval, "library(SeuratWrappers)")
        await asyncio.to_thread(con.voidEval, "library(SeuratDisk)")
        await asyncio.to_thread(con.voidEval, read_loom_r_code)

        await asyncio.to_thread(con.voidEval, load_data_r_code)
        await asyncio.to_thread(con.voidEval, qc_r_code)
        if str(data["is_combined"]) == 'Y':
            await asyncio.to_thread(con.voidEval, combined_r_code)
        await asyncio.to_thread(con.voidEval, dc_r_code)
        await asyncio.to_thread(con.voidEval, anno_r_code)
        await asyncio.to_thread(con.voidEval, 'Idents(object = '+rbaseName+') <- "check_clusters"')

    else:
        async with lock_rserv:
            con = rserv_connections[sKey]
    
    # 2. output dc images
    elbow_png = "elbow_subtype_dc_" + str(uuid.uuid1()) + ".png"
    dim_png = "dim_subtype_dc_" + str(uuid.uuid1()) + ".png"
            
    await asyncio.to_thread(con.voidEval, 'subcluster <- subset('+rbaseName+', idents = "'+str(data["target"])+'")')
    await asyncio.to_thread(con.voidEval, 'subcluster <- RunPCA(subcluster, npcs = 50, verbose = FALSE)')
    await asyncio.to_thread(con.voidEval, 'p <- ElbowPlot(subcluster, ndims = 50)')
    await asyncio.to_thread(con.voidEval, 'subcluster <- RunUMAP(subcluster, reduction = "pca", dims = 1:'+str(data["dimension"])+')')
    await asyncio.to_thread(con.voidEval, 'subcluster <- FindNeighbors(subcluster, reduction = "pca", dims = 1:'+str(data["dimension"])+')')
    await asyncio.to_thread(con.voidEval, 'subcluster <- FindClusters(subcluster, resolution = '+str(data["resolution"])+')')
    await asyncio.to_thread(con.voidEval, 'ggsave("'+elbow_png+'", p, width=10, height=6, units="in")')
    await asyncio.to_thread(con.voidEval, 'p1 <- DimPlot(subcluster, reduction = "umap", group.by = "seurat_clusters")')
    await asyncio.to_thread(con.voidEval, 'ggsave("'+dim_png+'", p1, width=10, height=6, units="in")')
    if str(data["is_combined"]) == 'Y':
        dim2_png = "dim2_subtype_dc_" + str(uuid.uuid1()) + ".png"
        await asyncio.to_thread(con.voidEval, 'px <- DimPlot(subcluster, reduction = "umap", group.by = "type")+NoLegend()')
        await asyncio.to_thread(con.voidEval, 'ggsave("'+dim2_png+'", px, width=10, height=6, units="in")')
        result = [{'id':0, 'url': "/tmpImg/"+elbow_png},{'id':1, 'url': "/tmpImg/"+dim_png},{'id':2, 'url': "/tmpImg/"+dim2_png}]
    else:
        result = [{'id':0, 'url': "/tmpImg/"+elbow_png},{'id':1, 'url': "/tmpImg/"+dim_png}]

    return web.json_response(result)

async def scRNA_subtype_dc(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    if data["is_combined"] == "Y":
        rbaseName = "project.combined"
    else:
        rbaseName = 'seurat_'+str(data["data_id"])
    sKey = str(data["user_id"]) + ':' + str(data['anno_cluster_id'])+ ':'+ str(data["target"])
    
    async with lock_rserv:
        conn = rserv_connections[sKey]

    base_dir = pathlib.Path(config["dataDir"]) / "annotation" / "subtype"
    userPath = base_dir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    while userPath.exists():
        userPath = base_dir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    await asyncio.to_thread(userPath.mkdir, parents=True)

    ncluster = await asyncio.to_thread(conn.eval, 'length(levels(subcluster$seurat_clusters))')
    ncluster = int(ncluster)
    # generate all marker file && png files
    await asyncio.to_thread(conn.voidEval, 'setwd("'+userPath.absolute().__str__()+'")')
    await asyncio.to_thread(conn.voidEval, 'markers <- FindAllMarkers(subcluster, only.pos = TRUE, min.pct = 0.25, logfc.threshold = 0.25)')
    await asyncio.to_thread(conn.voidEval, 'write.table(markers, file = "all_markers.xls", sep = "\t", row.names = F)')
    await asyncio.to_thread(conn.voidEval, 'ggsave("elbow.png", p, width=10, height=6, units="in")')
    await asyncio.to_thread(conn.voidEval, 'ggsave("dim.png", p1, width=10, height=6, units="in")')

    code_result = 'subcluster <- subset('+rbaseName+', idents = "'+str(data["target"])+'")\n'+\
    'subcluster <- RunPCA(subcluster, npcs = 50, verbose = FALSE)\n'+\
    'subcluster <- RunUMAP(subcluster, dims = 1:'+str(data["dimension"])+')\n' + \
    'subcluster <- RunTSNE(subcluster, dims = 1:'+str(data["dimension"])+')\n' + \
    'subcluster <- FindNeighbors(subcluster, reduction = "pca", dims = 1:'+str(data["dimension"])+')\n' + \
    'subcluster <- FindClusters(subcluster, resolution = '+str(data["resolution"])+')\n'

    async with request.app['mysql_db'].acquire() as conn:
        sql = "SELECT * FROM annotation_subtype a WHERE a.annotation_type_id = $anno_cluster_id AND a.input_cell_name = '$target';"
        sql = sql.replace("$anno_cluster_id", str(data["anno_cluster_id"]))
        sql = sql.replace("$target", str(data["target"]))
        res = await conn.execute(sql)
        row = await res.fetchone()
        if row:
            # 删除原本的分群结果目录
            rmtree(str(row["result_path"]))
            # 更新数据库中的分群结果
            async with conn.begin() as transaction:
                await conn.execute(annotation_subtype.update()
                    .where(annotation_subtype.c.annotation_type_id == int(data["anno_cluster_id"]))
                    .where(annotation_subtype.c.input_cell_name == str(data["target"]))
                    .values(
                    dc_r_code=code_result,
                    result_path=userPath.absolute().__str__(),
                    anno_r_code='',
                    str_anno='',
                    n_cluster=ncluster
                ))
                await transaction.commit()
        else:
            async with conn.begin() as transaction:
                await conn.execute(annotation_subtype.insert().values(
                    user_id = int(data["user_id"]),
                    annotation_type_id=str(data["anno_cluster_id"]),
                    input_cell_name=data["target"],
                    dc_r_code=code_result,
                    result_path=userPath.absolute().__str__(),
                    anno_r_code='',
                    n_cluster=ncluster,
                    str_anno='',
                    marker_gene_order = '',
                    cluster_name_order = ''
                ))
                await transaction.commit()

        return web.Response(text=str(ncluster)) # return number of clusters

async def show_subtype_anno_image(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    sKey = str(data["user_id"]) + ':' + str(data['anno_cluster_id'])+ ':'+ str(data["target"])
    
    async with lock_rserv:
        conn = rserv_connections[sKey]

    feature_png = "feature_subtype_" + str(uuid.uuid1()) + ".png"
    dim_png = "dim_subtype_" + str(uuid.uuid1()) + ".png"
    violin_png = "violin_subtype_" + str(uuid.uuid1()) + ".png"

    await asyncio.to_thread(conn.voidEval, 'setwd("'+str(config["tempImageDir"])+'")')
    await asyncio.to_thread(conn.voidEval, 'p2 <- DimPlot(subcluster, reduction = "umap", label = TRUE, repel = TRUE)')
    await asyncio.to_thread(conn.voidEval, 'ggsave("'+dim_png+'", p2, width=10, height=6, units="in")')
    await asyncio.to_thread(conn.voidEval, 'p3 <- VlnPlot(subcluster, features ="'+data["marker"]+'")')
    await asyncio.to_thread(conn.voidEval, 'ggsave("'+violin_png+'", p3, width=10, height=6, units="in")')
    await asyncio.to_thread(conn.voidEval, 'p4 <- FeaturePlot(subcluster, features ="'+data["marker"]+'")')
    await asyncio.to_thread(conn.voidEval, 'ggsave("'+feature_png+'", p4, width=10, height=6, units="in")')
    if str(data["is_combined"]) == 'Y':
        dim2_png = "dim2_subtype_dc_" + str(uuid.uuid1()) + ".png"
        await asyncio.to_thread(conn.voidEval, 'px <- DimPlot(subcluster, reduction = "umap", group.by = "type")+NoLegend()')
        await asyncio.to_thread(conn.voidEval, 'ggsave("'+dim2_png+'", px, width=10, height=6, units="in")')
        results = [{'id':0, 'url': "/tmpImg/"+dim_png},{'id':1, 'url': "/tmpImg/"+feature_png},{'id':2, 'url': "/tmpImg/"+violin_png},{'id':3, 'url': "/tmpImg/"+dim2_png}]
    else:
        results = [{'id':0, 'url': "/tmpImg/"+dim_png},{'id':1, 'url': "/tmpImg/"+feature_png},{'id':2, 'url': "/tmpImg/"+violin_png}]
    
    return web.json_response(results)

async def download_subtype_marker_file(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    

    data_id = int(data["anno_cluster_id"])
    cell_name = str(data["target"])


    async with request.app['mysql_db'].acquire() as conn:
        res = await conn.execute(annotation_subtype.select()
                .where(annotation_subtype.c.annotation_type_id == data_id)
                .where(annotation_subtype.c.input_cell_name == cell_name)
            )
        row = await res.fetchone()

        filename = 'all_markers.xls'
        filepath = str(row["result_path"]) + "/" + filename
        async with aiofiles.open(filepath, 'rb') as f:
            file_data = await f.read()
        headers = {
            'Content-Disposition': f'attachment; filename="{filename}"',
            'Content-Type': 'application/vnd.ms-excel',
        }
        return web.Response(body=file_data, headers=headers)

async def scRNA_subtype_anno(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    sKey = str(data["user_id"]) + ':' + str(data['anno_cluster_id'])+ ':'+ str(data["target"])
    
    async with lock_rserv:
        con = rserv_connections[sKey]

    # get result path
    async with request.app['mysql_db'].acquire() as conn:
        res00 = await conn.execute(annotation_subtype.select()
                .where(annotation_subtype.c.annotation_type_id==int(data["anno_cluster_id"]))
                .where(annotation_subtype.c.input_cell_name == str(data["target"]))
        )
        row00 = await res00.fetchone()
        result_path = str(row00['result_path'])

    ## generate anno_r_code
    anno_r_code = 'subcluster[["check_clusters"]] = ""\n'
    for index, anno in enumerate(str(data["anno"]).split(',')):
        anno_r_code += 'subcluster$check_clusters[subcluster$seurat_clusters == '+str(index)+'] = "'+anno+'"\n'

    cluster_order = '"'+str(data["cluster_name_order"]).replace(':', '","')+'"'
    marker_gene_order = '"'+str(data["marker_gene_order"]).replace(':', '","')+'"'
    
    # output result images
    await asyncio.to_thread(con.voidEval, 'setwd("'+result_path+'")')
    await asyncio.to_thread(con.voidEval, anno_r_code)
    await asyncio.to_thread(con.voidEval, 'ggsave("elbow.png", p, width=10, height=6, units="in")')
    await asyncio.to_thread(con.voidEval, 'Idents(object = subcluster) <- "check_clusters"')
    await asyncio.to_thread(con.voidEval, 'cluster_order <- c('+cluster_order+')')
    await asyncio.to_thread(con.voidEval, 'levels(subcluster) <- cluster_order')
    await asyncio.to_thread(con.voidEval, 'ph <- '+seurat_DoHeatmap.replace("$marker_gene_order", marker_gene_order).replace("$rbaseName", "subcluster"))
    await asyncio.to_thread(con.voidEval, 'pv <- '+seurat_VlnPlot.replace("$marker_gene_order", marker_gene_order).replace("$rbaseName", 'subcluster'))
    await asyncio.to_thread(con.voidEval, 'ggsave("marker_vln.png", pv, width=10, height=6, units="in")')
    await asyncio.to_thread(con.voidEval, 'ggsave("marker_heatmap.png", ph, width=10, height=6, units="in")')
    await asyncio.to_thread(con.voidEval, 'ggsave("dim1.png", p1, width=10, height=6, units="in")')
    await asyncio.to_thread(con.voidEval, 'p4 <- DimPlot(subcluster, reduction = "umap", label = T) + NoLegend()')
    await asyncio.to_thread(con.voidEval, 'ggsave("dim2.png", p4, width=10, height=6, units="in")')

    # close Rserv connection
    con.close()
    async with lock_rserv:
        del rserv_connections[sKey]

    str_anno_result = str(data["anno"]).replace(',', ':')
    async with request.app['mysql_db'].acquire() as conn:
        async with conn.begin() as transaction:
            await conn.execute(annotation_subtype.update()
                .where(annotation_subtype.c.annotation_type_id==int(data["anno_cluster_id"]))
                .where(annotation_subtype.c.input_cell_name == str(data["target"]))
                .values(
                    anno_r_code=anno_r_code,
                    str_anno = str_anno_result,
                    cluster_name_order = str(data["cluster_name_order"]),
                    marker_gene_order = str(data["marker_gene_order"])
            ))
            await transaction.commit()

    return web.Response(text="OK")

####################################### downstream annalysis ###################################################

async def query_downstream_data(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)

    # 1. merge -> dc -> anno_type -> anno_subtype
    # 2. merge -> dc -> anno_type
    # 3. integrate -> dc -> anno_type -> anno_subtype
    # 4. integrate -> dc -> anno_type
    # 5. cellranger -> qc -> dc -> anno_type -> anno_subtype
    # 6. cellranger -> qc -> dc -> anno_type
    # 7. matrix -> qc -> dc -> anno_type -> anno_subtype
    # 8. matrix -> qc -> dc -> anno_type
    # 9. fastq -> reads mapping -> generate loom -> qc -> dc -> anno_type -> anno_subtype
    # 10. fastq -> reads mapping -> generate loom -> qc -> dc -> anno_type
    # 11. fastq -> reads mapping -> qc -> dc -> anno_type -> anno_subtype
    # 12. fastq -> reads mapping -> qc -> dc -> anno_type

    results = []
    async with request.app['mysql_db'].acquire() as conn:
        # 2. merge -> dc -> anno_type
        sql2 = '''
            SELECT a.id, a.input_data_ids, b.id, c.id, c.cluster_name_order
            FROM upstream_merge a, upstream_dc b, annotation_type c
            WHERE a.id = b.input_data_id
                  AND b.input_data_type = 'upstream_merge' 
                  AND b.id = c.upstream_dc_id
                  AND a.user_id = $user_id;
        '''
        sql2 = sql2.replace("$user_id", str(data['user_id']))
        res2 = await conn.execute(sql2)
        async for row2 in res2:
            results.append({
                "data_id": "-",
                "species": "-",
                "data_type": "-",
                "reads_mapping_id": "-",
                "generate_loom_id": "-",
                "quality_control_id": "-",
                "combined_id": str(row2[0]),
                "combined_type": "merge",
                "combined_data_ids": row2[1],
                "dc_id": str(row2[2]),
                "anno_cluster_id": str(row2[3]),
                "anno_cluster_result": row2[4],
                "anno_subcluster_id": "-",
                "anno_subcluster_name": "-",
                "anno_subcluster_result": "-"
            })
            
        # 1. merge -> dc -> anno_type -> anno_subtype
        sql = '''
            SELECT a.id, a.input_data_ids, b.id, c.id, c.cluster_name_order,
                   d.id, d.input_cell_name, d.cluster_name_order
            FROM upstream_merge a, upstream_dc b, annotation_type c, annotation_subtype d
            WHERE a.id = b.input_data_id
                  AND b.input_data_type = 'upstream_merge' 
                  AND b.id = c.upstream_dc_id
                  AND c.id = d.annotation_type_id
                  AND a.user_id = $user_id;
            '''
        sql = sql.replace("$user_id", str(data['user_id']))
        res = await conn.execute(sql)
        async for row in res:
            results.append({
                "data_id": "-",
                "species": "-",
                "data_type": "-",
                "reads_mapping_id": "-",
                "generate_loom_id": "-",
                "quality_control_id": "-",
                "combined_id": str(row[0]),
                "combined_type": "merge",
                "combined_data_ids": row[1],
                "dc_id": str(row[2]),
                "anno_cluster_id": str(row[3]),
                "anno_cluster_result": row[4],
                "anno_subcluster_id": str(row[5]),
                "anno_subcluster_name": row[6],
                "anno_subcluster_result": row[7]
            })

        # 4. integrate -> dc -> anno_type
        sql4 = '''
            SELECT a.id, a.input_data_ids, b.id, c.id, c.cluster_name_order
            FROM upstream_integrate a, upstream_dc b, annotation_type c
            WHERE a.id = b.input_data_id 
                  AND b.input_data_type = 'upstream_integrate' 
                  AND b.id = c.upstream_dc_id
                  AND a.user_id = $user_id;
        '''
        sql4 = sql4.replace("$user_id", str(data['user_id']))
        res4 = await conn.execute(sql4)
        async for row4 in res4:
            results.append({
                "data_id": "-",
                "species": "-",
                "data_type": "-",
                "reads_mapping_id": "-",
                "generate_loom_id": "-",
                "quality_control_id": "-",
                "combined_id": str(row4[0]),
                "combined_type": "integrate",
                "combined_data_ids": row4[1],
                "dc_id": str(row4[2]),
                "anno_cluster_id": str(row4[3]),
                "anno_cluster_result": row4[4],
                "anno_subcluster_id": "-",
                "anno_subcluster_name": "-",
                "anno_subcluster_result": "-"
            })    

        # 3. integrate -> dc -> anno_type -> anno_subtype
        sql3 = '''
            SELECT a.id, a.input_data_ids, b.id, c.id, c.cluster_name_order,
                   d.id, d.input_cell_name, d.cluster_name_order
            FROM upstream_integrate a, upstream_dc b, annotation_type c, annotation_subtype d
            WHERE a.id = b.input_data_id 
                  AND b.input_data_type = 'upstream_integrate' 
                  AND b.id = c.upstream_dc_id
                  AND c.id = d.annotation_type_id
                  AND a.user_id = $user_id;
        '''
        sql3 = sql3.replace("$user_id", str(data['user_id']))
        res3 = await conn.execute(sql3)
        async for row3 in res3:
            results.append({
                "data_id": "-",
                "species": "-",
                "data_type": "-", 
                "reads_mapping_id": "-",
                "generate_loom_id": "-",
                "quality_control_id": "-",
                "combined_id": str(row3[0]),
                "combined_type": "integrate",
                "combined_data_ids": row3[1],
                "dc_id": str(row3[2]),
                "anno_cluster_id": str(row3[3]),
                "anno_cluster_result": row3[4],
                "anno_subcluster_id": str(row3[5]),
                "anno_subcluster_name": row3[6],
                "anno_subcluster_result": row3[7]
            })

        # 6. cellranger -> qc -> dc -> anno_type
        sql6 = '''
            SELECT a.id, a.species, a.data_type, b.id, c.id, d.id, d.cluster_name_order
            FROM upload_data a, upstream_quality_control b, upstream_dc c, annotation_type d
            WHERE b.input_data_type = 'upload_data_cellranger'
                  AND a.id = b.input_data_id
                  AND b.id = c.input_data_id
                  AND c.input_data_type = 'upstream_quality_control'
                  AND c.id = d.upstream_dc_id
                  AND a.user_id = $user_id;
        '''
        sql6 = sql6.replace("$user_id", str(data['user_id']))
        res6 = await conn.execute(sql6)
        async for row6 in res6:
            results.append({
                "data_id": str(row6[0]),
                "species": row6[1],
                "data_type": row6[2],
                "reads_mapping_id": "-",
                "generate_loom_id": "-",
                "quality_control_id": str(row6[3]),
                "combined_id": "-",
                "combined_type": "-",
                "combined_data_ids": "-",
                "dc_id": str(row6[4]),
                "anno_cluster_id": str(row6[5]),
                "anno_cluster_result": row6[6],
                "anno_subcluster_id": "-",
                "anno_subcluster_name": "-",
                "anno_subcluster_result": "-"
            })

        # 5 cellranger -> qc -> dc -> anno_type -> anno_subtype
        sql5 = '''
            SELECT a.id, a.species, a.data_type, b.id, c.id, d.id, d.cluster_name_order,
                   e.id, e.input_cell_name, e.cluster_name_order
            FROM upload_data a, upstream_quality_control b, upstream_dc c, annotation_type d,
                 annotation_subtype e
            WHERE b.input_data_type = 'upload_data_cellranger'
                  AND a.id = b.input_data_id
                  AND b.id = c.input_data_id
                  AND c.input_data_type = 'upstream_quality_control'
                  AND c.id = d.upstream_dc_id
                  AND d.id = e.annotation_type_id
                  AND a.user_id = $user_id;
            '''
        sql5 = sql5.replace("$user_id", str(data['user_id']))
        res5 = await conn.execute(sql5)
        async for row5 in res5:
            results.append({
                "data_id": str(row5[0]),
                "species": row5[1],
                "data_type": row5[2],
                "reads_mapping_id": "-",
                "generate_loom_id": "-",
                "quality_control_id": str(row5[3]),
                "combined_id": "-",
                "combined_type": "-",
                "combined_data_ids": "-",
                "dc_id": str(row5[4]),
                "anno_cluster_id": str(row5[5]),
                "anno_cluster_result": row5[6],
                "anno_subcluster_id": str(row5[7]),
                "anno_subcluster_name": row5[8],
                "anno_subcluster_result": row5[9]
            })
        
        # 8 matrix -> qc -> dc -> anno_type
        sql8 = '''
            SELECT a.id, a.species, a.data_type, b.id, c.id, d.id, d.cluster_name_order
            FROM upload_data a, upstream_quality_control b, upstream_dc c, annotation_type d
            WHERE b.input_data_type = 'upload_data_matrix'
                  AND a.id = b.input_data_id
                  AND b.id = c.input_data_id
                  AND c.id = d.upstream_dc_id
                  AND c.input_data_type = 'upstream_quality_control'
                  AND a.user_id = $user_id;
        '''
        sql8 = sql8.replace("$user_id", str(data['user_id']))
        res8 = await conn.execute(sql8)
        async for row8 in res8:
            results.append({
                "data_id": str(row8[0]),
                "species": row8[1],
                "data_type": row8[2],
                "reads_mapping_id": "-",
                "generate_loom_id": "-",
                "quality_control_id": str(row8[3]),
                "combined_id": "-",
                "combined_type": "-",
                "combined_data_ids": "-",
                "dc_id": str(row8[4]),
                "anno_cluster_id": str(row8[5]),
                "anno_cluster_result": row8[6],
                "anno_subcluster_id": "-",
                "anno_subcluster_name": "-",
                "anno_subcluster_result": "-"
            })

        # 7 matrix -> qc -> dc -> anno_subtype
        sql7 = '''
            SELECT a.id, a.species, a.data_type, b.id, c.id, d.id, d.cluster_name_order,
                   e.id, e.input_cell_name, e.cluster_name_order
            FROM upload_data a, upstream_quality_control b, upstream_dc c, annotation_type d,
                 annotation_subtype e
            WHERE b.input_data_type = 'upload_data_matrix'
                AND a.id = b.input_data_id
                AND b.id = c.input_data_id
                AND c.id = d.upstream_dc_id
                AND c.input_data_type = 'upstream_quality_control'
                AND d.id = e.annotation_type_id
                AND a.user_id = $user_id;
        '''
        sql7 = sql7.replace("$user_id", str(data['user_id']))
        res7 = await conn.execute(sql7)
        async for row7 in res7:
            results.append({
                "data_id": str(row7[0]),
                "species": row7[1],
                "data_type": row7[2],
                "reads_mapping_id": "-",
                "generate_loom_id": "-",
                "quality_control_id": str(row7[3]),
                "combined_id": "-",
                "combined_type": "-",
                "combined_data_ids": "-",
                "dc_id": str(row7[4]),
                "anno_cluster_id": str(row7[5]),
                "anno_cluster_result": row7[6],
                "anno_subcluster_id": str(row7[7]),
                "anno_subcluster_name": row7[8],
                "anno_subcluster_result": row7[9]
            })

        # 10. fastq -> reads mapping -> generate loom -> qc -> dc -> anno_type
        sql10 = '''
            SELECT a.id, a.species, a.data_type, b.id, c.id, d.id, e.id, f.id, f.cluster_name_order
            FROM upload_data a, upstream_reads_mapping b, 
                 upstream_generate_loom c, upstream_quality_control d,
                 upstream_dc e, annotation_type f
            WHERE a.id = b.upload_data_id 
                  AND b.id = c.upstream_reads_mapping_id 
                  AND c.id = d.input_data_id 
                  AND d.id = e.input_data_id 
                  AND d.input_data_type = 'upstream_generate_loom'
                  AND e.id = f.upstream_dc_id
                  AND e.input_data_type = 'upstream_quality_control'
                  AND a.user_id = $user_id;
        ''' 
        sql10 = sql10.replace("$user_id", str(data['user_id']))
        res10 = await conn.execute(sql10)
        async for row10 in res10:
            results.append({
                "data_id": str(row10[0]),
                "species": row10[1],
                "data_type": row10[2],
                "reads_mapping_id": str(row10[3]),
                "generate_loom_id": str(row10[4]),
                "quality_control_id": str(row10[5]),
                "combined_id": "-",
                "combined_type": "-",
                "combined_data_ids": "-",
                "dc_id": str(row10[6]),
                "anno_cluster_id": str(row10[7]),
                "anno_cluster_result": row10[8],
                "anno_subcluster_id": "-",
                "anno_subcluster_name": "-",
                "anno_subcluster_result": "-"
            })

        # 9. fastq -> reads mapping -> generate loom -> qc -> dc -> anno_type -> anno_subtype
        sql9 = '''
            SELECT a.id, a.species, a.data_type, b.id, c.id, d.id, e.id, f.id, f.cluster_name_order,
                   g.id, g.input_cell_name, g.cluster_name_order
            FROM upload_data a, upstream_reads_mapping b,
                 upstream_generate_loom c, upstream_quality_control d,
                 upstream_dc e, annotation_type f, annotation_subtype g
            WHERE a.id = b.upload_data_id
                  AND b.id = c.upstream_reads_mapping_id
                  AND c.id = d.input_data_id
                  AND d.id = e.input_data_id
                  AND d.input_data_type = 'upstream_generate_loom'
                  AND e.id = f.upstream_dc_id
                  AND e.input_data_type = 'upstream_quality_control'
                  AND f.id = g.annotation_type_id
                  AND a.user_id = $user_id;
        '''
        sql9 = sql9.replace("$user_id", str(data['user_id']))
        res9 = await conn.execute(sql9)
        async for row9 in res9:
            results.append({
                "data_id": str(row9[0]),
                "species": row9[1],
                "data_type": row9[2],
                "reads_mapping_id": str(row9[3]),
                "generate_loom_id": str(row9[4]),
                "quality_control_id": str(row9[5]),
                "combined_id": "-",
                "combined_type": "-",
                "combined_data_ids": "-",
                "dc_id": str(row9[6]),
                "anno_cluster_id": str(row9[7]),
                "anno_cluster_result": row9[8],
                "anno_subcluster_id": str(row9[9]),
                "anno_subcluster_name": row9[10],
                "anno_subcluster_result": row9[11]
            })

        # 12. fastq -> reads mapping -> qc -> dc -> anno_type
        sql12 = '''
            SELECT a.id, a.species, a.data_type, b.id, c.id, d.id, e.id, e.cluster_name_order
            FROM upload_data a, upstream_reads_mapping b,
                 upstream_quality_control c, upstream_dc d,
                 annotation_type e
            WHERE a.id = b.upload_data_id 
                  AND b.id = c.input_data_id 
                  AND c.input_data_type = 'upstream_reads_mapping' 
                  AND c.id = d.input_data_id 
                  AND d.id = e.upstream_dc_id
                  AND d.input_data_type = 'upstream_quality_control'
                  AND a.user_id = $user_id;
        '''
        sql12 = sql12.replace("$user_id", str(data['user_id']))
        res12 = await conn.execute(sql12)
        async for row12 in res12:
            results.append({
                "data_id": str(row12[0]),
                "species": row12[1],
                "data_type": row12[2],
                "reads_mapping_id": str(row12[3]),
                "generate_loom_id": "-",
                "quality_control_id": str(row12[4]),
                "combined_id": "-",
                "combined_type": "-",
                "combined_data_ids": "-",
                "dc_id": str(row12[5]),
                "anno_cluster_id": str(row12[6]),
                "anno_cluster_result": row12[7],
                "anno_subcluster_id": "-",
                "anno_subcluster_name": "-",
                "anno_subcluster_result": "-"
            })

        # 11. fastq -> reads mapping -> qc -> dc -> anno_type -> anno_subtype
        sql11 = '''
            SELECT a.id, a.species, a.data_type, b.id, c.id, d.id, e.id, e.cluster_name_order,
                   f.id, f.input_cell_name, f.cluster_name_order
            FROM upload_data a, upstream_reads_mapping b,
                 upstream_quality_control c, upstream_dc d,
                 annotation_type e, annotation_subtype f
            WHERE a.id = b.upload_data_id
                  AND b.id = c.input_data_id
                  AND c.input_data_type = 'upstream_reads_mapping'
                  AND c.id = d.input_data_id
                  AND d.id = e.upstream_dc_id
                  AND d.input_data_type = 'upstream_quality_control'
                  AND e.id = f.annotation_type_id
                  AND a.user_id = $user_id;
            '''
        sql11 = sql11.replace("$user_id", str(data['user_id']))
        res11 = await conn.execute(sql11)
        async for row11 in res11:
            results.append({
                "data_id": str(row11[0]),
                "species": row11[1],
                "data_type": row11[2],
                "reads_mapping_id": str(row11[3]),
                "generate_loom_id": "-",
                "quality_control_id": str(row11[4]),
                "combined_id": "-",
                "combined_type": "-",
                "combined_data_ids": "-",
                "dc_id": str(row11[5]),
                "anno_cluster_id": str(row11[6]),
                "anno_cluster_result": row11[7],
                "anno_subcluster_id": str(row11[8]),
                "anno_subcluster_name": row11[9],
                "anno_subcluster_result": row11[10]
            })

        return web.json_response(results)

# according to this https://github.com/broadinstitute/infercnv/issues/170#issuecomment-524073431
# inferCNV can not apply for merge/integrate seurat object 
# instead, inferCNV should apply for each data 
async def scRNA_infereCNV(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    rbaseName = 'seurat_'+ str(data["data_id"])
    load_data_r_code = ''
    dc_r_code = ''
    qc_r_code = ''
    anno_type_r_code = ''
    anno_subtype_r_code = ''
    infereCNV_r_code = ''

    async with request.app['mysql_db'].acquire() as conn:
        sql = '''
            SELECT a.r_code, b.r_code, c.r_code
            FROM annotation_type a, upstream_dc b, upstream_quality_control c
            WHERE a.upstream_dc_id = b.id AND a.id = $anno_cluster_id AND c.id = b.input_data_id;
        '''
        sql = sql.replace("$anno_cluster_id", str(data["anno_cluster_id"]))
        res = await conn.execute(sql)
        row = await res.fetchone()
        anno_type_r_code = row[0]
        dc_r_code = row[1]
        qc_r_code = row[2]

        # get load_data_r_code
        if str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) == "-":
            res1 = await conn.execute(upstream_reads_mapping.select().
                    where(upstream_reads_mapping.c.upload_data_id == int(data["reads_mapping_id"]))
                )
            row1 = await res1.fetchone()
            load_data_r_code = rbaseName+'.data <- Read10X("'+row1["result_path"]+'")\n'+\
            rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
            rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
            rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
            
        elif str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) != "-":
            res1 = await conn.execute(upstream_generate_loom.select()
                    .where(upstream_generate_loom.c.id == int(data["generate_loom_id"]))
                )
            row1 = await res1.fetchone()
            loom_file = row1['result_path'] + '/cellranger.loom'
            load_data_r_code = rbaseName+' <- read.loom.matrices(file = "'+loom_file+'")\n'+\
                rbaseName+' <- as.Seurat('+rbaseName+')\n'+\
                rbaseName+'[["RNA"]] <- '+rbaseName+'[["spliced"]]\n'+\
                'DefaultAssay('+rbaseName+') <- "RNA"\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

        elif str(data["data_type"]) == 'cellranger':
            res1 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
            row1 = await res1.fetchone()
            load_data_r_code = rbaseName+'.data <- Read10X("'+row1["upload_path"]+'")\n'+\
            rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
            rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
            rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

        elif str(data["data_type"]) == 'matrix':
            res1 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
            row1 = await res1.fetchone()
            load_data_r_code = rbaseName+'.data <- read.table(file="'+row1["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
            rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
            rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
            rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
        else:
            pass
        
        # get anno_subtype_r_code
        if str(data["anno_subcluster_id"]) != "-":
            rbaseName = 'subcluster'
            res2 =  await conn.execute(annotation_subtype.select()
                    .where(annotation_subtype.c.id == int(data["anno_subcluster_id"]))
                )
            row2 = res2.fetchone()
            anno_subtype_r_code = row2["dc_r_code"] + '\n' + row2["anno_r_code"] + '\n'
    
    # 3. generate inferCNV_r_code
    ref_group = ''
    if str(data["ref_group_names"]).count(',') == 0:
        ref_group = '"'+str(data["ref_group_names"])+ '"'
    else:
        ref_group = '"'+str(data["ref_group_names"]).replace(",", '","') + '"'

    infereCNV_r_code = 'celltype <- cbind(colnames('+rbaseName+'),as.character('+rbaseName+'@meta.data$check_clusters))\n'+\
        'write.table(celltype, "celltype.txt", sep = "\t",row.names=F,col.names=F,quote=F)\n'+\
        'mtx <- '+rbaseName+'@assays$RNA@counts\n'+\
        'infercnv_obj = CreateInfercnvObject(raw_counts_matrix = mtx,annotations_file ="celltype.txt",gene_order_file="'+config["gene_order_file"]+'",ref_group_names=c('+ref_group+'))\n'+\
        'infercnv_obj = infercnv::run(infercnv_obj,cutoff='+str(data["cutoff"])+',out_dir="./",cluster_by_groups=TRUE,denoise=TRUE,HMM=TRUE,num_threads='+str(data["num_threads"])+')'

    # 4. generate script file and submit slurm job
    script = r_script_inferCNV.replace("#loadDataRcode", load_data_r_code)
    script = script.replace("#qcRcode", qc_r_code)
    script = script.replace("#dcRcode", dc_r_code)
    script = script.replace("#annotationTypeRcode", anno_type_r_code)
    if str(data["anno_subcluster_id"]) != "-":
        script = script.replace("#annotationSubtypeRcode", anno_subtype_r_code)
    script = script.replace("#inferCNVRcode", infereCNV_r_code)

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode='w',
        prefix="inferCNV_",
        suffix=".R",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file:  
        await file.write(script)

    baseDir = pathlib.Path(config["inferCnvResultPath"])
    userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    while userDir.exists():
        userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    await asyncio.to_thread(userDir.mkdir, parents=True)

    downstreamCode = "export PATH=/home/deng/miniconda3/envs/inferCNV/bin:$PATH\n"+\
                     "export LD_LIBRARY_PATH=/home/deng/miniconda3/envs/inferCNV/lib:$LD_LIBRARY_PATH\n"+\
                     "/usr/bin/Rscript " + file.name

    script1 = downstream_bash.replace("partition", config["slurmPartition"])
    script1 = script1.replace("jobLogPath", config["slurmJobLogPath"])
    script1 = script1.replace("downstreamCode", downstreamCode)
    script1 = script1.replace("userDir", userDir.absolute().__str__())
    script1 = script1.replace("$localcores", str(data["num_threads"]))

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="inferCNV_",
        suffix=".sh",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file1:
        await file1.write(script1)

    """
    cmd = "sbatch "+file1.name
    proc = await asyncio.create_subprocess_shell(
        cmd, 
        stdout=asyncio.subprocess.PIPE, 
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    x = stdout.decode().split()
    jobID = int(x[-1])
    """

    result = await asyncio.to_thread(subprocess.check_output, ['/usr/local/bin/sbatch', str(file1.name)])
    output = result.decode('utf-8')
    x = output.split()
    jobID = int(x[-1])
    
    # 5. insert value to downstream table
    async with lock:
        slurm_jobs[jobID] = 'inferCNV'

    if str(data["anno_subcluster_id"]) != "-":
        input_data_type = "annotation_subtype"
        annotation_id = int(data["anno_subcluster_id"])
    else:
        input_data_type = "annotation_type"
        annotation_id = int(data["anno_cluster_id"])

    async with request.app['mysql_db'].acquire() as conn:
        resx = await conn.execute(downstream.select()
                .where((downstream.c.annotation_id == annotation_id) &
                       (downstream.c.input_data_type == input_data_type) &
                       (downstream.c.analysis_type == "inferCNV")
                )
            )
        rowx = await resx.fetchone()
        if rowx:
            async with conn.begin() as transaction:
                await conn.execute(downstream.update()
                    .where((downstream.c.annotation_id == annotation_id) &
                        (downstream.c.input_data_type == input_data_type) &
                        (downstream.c.analysis_type == "inferCNV")
                    )
                    .values(
                        slurm_job_id = jobID,
                        slurm_job_state = "PENDING",
                        result_path = userDir.absolute().__str__(),
                    )
                )
                await transaction.commit()
        else:
            async with conn.begin() as transaction:
                await conn.execute(downstream.insert().values(
                    user_id = int(data["user_id"]),
                    annotation_id = annotation_id,
                    input_data_type = input_data_type,
                    slurm_job_id = jobID,
                    slurm_job_state = "PENDING",
                    analysis_type = "inferCNV",
                    result_path = userDir.absolute().__str__(),
                ))
                await transaction.commit()

        return web.Response(text="OK")

async def scRNA_pearson_correlation(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    load_data_r_code = ''
    qc_r_code = ''
    combined_r_code = ''
    dc_r_code = ''
    anno_type_r_code = ''
    anno_subtype_r_code = ''
    correlation_r_code = ''
    
    # 1. 
    async with request.app['mysql_db'].acquire() as conn:
        sql = '''
            SELECT a.r_code, b.r_code
            FROM annotation_type a, upstream_dc b
            WHERE a.upstream_dc_id = b.id AND a.id = $anno_cluster_id;
        '''
        sql = sql.replace("$anno_cluster_id", str(data["anno_cluster_id"]))
        res = await conn.execute(sql)
        row = await res.fetchone()
        anno_type_r_code = row[0]
        dc_r_code = row[1]

        if str(data["anno_subcluster_id"]) != '-':
            res1 = await conn.execute(annotation_subtype.select()
                .where(annotation_subtype.c.id == int(data["anno_subcluster_id"]))
            )
            row1 = await res1.fetchone()
            anno_subtype_r_code = row1["dc_r_code"] + '\n' + row1["anno_r_code"] + '\n'

        if str(data["combined_id"]) != '-':
            # get combined_r_code
            if str(data["combined_type"]) == 'merge':
                res2 = await conn.execute(upstream_merge.select()
                    .where(upstream_merge.c.id == int(data["combined_id"]))
                )
                row2 = await res2.fetchone()
                combined_r_code = row2["r_code"]
            else:
                res2 = await conn.execute(upstream_integrate.select()
                    .where(upstream_integrate.c.id == int(data["combined_id"]))
                )
                row2 = await res2.fetchone()
                combined_r_code = row2["r_code"]

            # get qc_r_code load_data_r_code
            # merge/integrate data are not generated from generate_loom result
            for x in str(data["combined_data_ids"]).split(':'):
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(x)))
                row3 = await res3.fetchone()
                rbaseName = 'seurat_' + x

                if row3["data_type"] == "fastq":
                    res4 = await conn.execute(upstream_reads_mapping.select()
                        .where(upstream_reads_mapping.c.upload_data_id == int(row3["id"]))
                    )
                    row4 = await res4.fetchone()
                    load_data_r_code += rbaseName+'.data <- Read10X("'+row4["result_path"]+'")\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res5 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.input_data_id == int(row4["id"]))
                    )
                    row5 = await res5.fetchone()
                    qc_r_code += row5["r_code"]+"\n"

                elif row3["data_type"] == "cellranger":
                    load_data_r_code += rbaseName+'.data <- Read10X("'+row3["upload_path"]+'")\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res4 = await conn.execute(upstream_quality_control.select()
                            .where(upstream_quality_control.c.input_data_id == int(row3["id"]))
                        )
                    row4 = await res4.fetchone()
                    qc_r_code += row4["r_code"] + "\n"

                else: # row3["data_type"] == "matrix"
                    load_data_r_code = rbaseName+'.data <- read.table(file="'+row3["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res4 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.input_data_id == row3["id"])
                    )
                    row4 = await res4.fetchone()
                    qc_r_code += row4["r_code"] + "\n"

        else: # str(data["combined_id"]) == '-':
            # get qc_r_code
            res2 = await conn.execute(upstream_quality_control.select()
                .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
            )
            row2 = await res2.fetchone()
            qc_r_code = row2["r_code"]
            # get load_data_r_code
            rbaseName = 'seurat_' + str(data["data_id"])
            if str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) == "-":
                res3 = await conn.execute(upstream_reads_mapping.select().
                        where(upstream_reads_mapping.c.upload_data_id == int(data["reads_mapping_id"]))
                    )
                row3 = await res3.fetchone()
                load_data_r_code = rbaseName+'.data <- Read10X("'+row3["result_path"]+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                
            elif str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) != "-":
                res3 = await conn.execute(upstream_generate_loom.select()
                        .where(upstream_generate_loom.c.id == int(data["generate_loom_id"]))
                    )
                row3 = await res3.fetchone()
                loom_file = row3['result_path'] + '/cellranger.loom'
                load_data_r_code = rbaseName+' <- read.loom.matrices(file = "'+loom_file+'")\n'+\
                    rbaseName+' <- as.Seurat('+rbaseName+')\n'+\
                    rbaseName+'[["RNA"]] <- '+rbaseName+'[["spliced"]]\n'+\
                    'DefaultAssay('+rbaseName+') <- "RNA"\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

            elif str(data["data_type"]) == 'cellranger':
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                row3 = await res3.fetchone()
                load_data_r_code = rbaseName+'.data <- Read10X("'+row3["upload_path"]+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

            elif str(data["data_type"]) == 'matrix':
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                row3 = await res3.fetchone()
                load_data_r_code = rbaseName+'.data <- read.table(file="'+row3["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
            else:
                pass

    # 3. generate corelation_r_code
    if str(data["anno_subcluster_id"]) == '-':
        if data["combined_type"] != '-':
            correlation_r_code = 'Idents(project.combined) <- "check_clusters"\n'+\
            'av.exp <- AverageExpression(project.combined)$RNA\n'+\
            'cor.exp <- as.matrix(cor(av.exp))\n'+\
            'p <- Heatmap(cor.exp, name = "cor.exp")\n'+\
            'png("anno_pearson_correlation.png", width = 12, height = 9, units="in", res=300)\n'+\
            'draw(p)\n'+\
            'dev.off()\n'
        else:
            correlation_r_code = 'Idents('+rbaseName+') <- "check_clusters"\n'+\
            'av.exp <- AverageExpression('+rbaseName+')$RNA\n'+\
            'cor.exp <- as.matrix(cor(av.exp))\n'+\
            'p <- Heatmap(cor.exp, name = "cor.exp")\n'+\
            'png("anno_pearson_correlation.png", width = 12, height = 9, units="in", res=300)\n'+\
            'draw(p)\n'+\
            'dev.off()\n'
    else:# data["anno_type"] == 'subcluster_annotation'
        correlation_r_code = 'Idents(subcluster) <- "check_clusters"\n'+\
            'av.exp <- AverageExpression(subcluster)$RNA\n'+\
            'cor.exp <- as.matrix(cor(av.exp))\n'+\
            'p <- Heatmap(cor.exp, name = "cor.exp")\n'+\
            'png("anno_pearson_correlation.png", width = 12, height = 9, units="in", res=300\n'+\
            'draw(p)\n'+\
            'dev.off()\n'

    # 4. generate script file and submit slurm job
    script = r_script_correlation.replace("#loadDataRcode", load_data_r_code)
    script = script.replace("#qcRcode", qc_r_code)
    if data["combined_type"] != '-':
        script = script.replace("#combinedRcode", combined_r_code)
    script = script.replace("#dcRcode", dc_r_code)
    script = script.replace("#annotationTypeRcode", anno_type_r_code)
    if str(data["anno_subcluster_id"]) != '-':
        script = script.replace("#annotationSubtypeRcode", anno_subtype_r_code)
    script = script.replace("#correlationRcode", correlation_r_code)

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="correlation",
        suffix=".R",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file:
        await file.write(script)

    baseDir = pathlib.Path(config["correlationResultPath"])
    userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    while userDir.exists():
        userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    await asyncio.to_thread(userDir.mkdir, parents=True)
    
    script1 = downstream_bash.replace("partition", config["slurmPartition"])
    script1 = script1.replace("jobLogPath", config["slurmJobLogPath"])
    script1 = script1.replace("downstreamCode", "/usr/bin/Rscript " + file.name)
    script1 = script1.replace("userDir", userDir.absolute().__str__())
    script1 = script1.replace("$localcores", '1')

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="correlation",
        suffix=".sh",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file1:
        await file1.write(script1)

    """
    the following code didnot work, I don't know why

    cmd = "sbatch "+str(file1.name)
    proc = await asyncio.create_subprocess_shell(
        cmd, 
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    """

    result = await asyncio.to_thread(subprocess.check_output, ['/usr/local/bin/sbatch', str(file1.name)])
    output = result.decode('utf-8')
    x = output.split()
    jobID = int(x[-1])
 
    # 5. insert value to downstream table
    async with lock:
        slurm_jobs[jobID] = 'correlation'

    if str(data["anno_subcluster_id"]) != "-":
        input_data_type = "annotation_subtype"
        annotation_id = int(data["anno_subcluster_id"])
    else:
        input_data_type = "annotation_type"
        annotation_id = int(data["anno_cluster_id"])

    async with request.app['mysql_db'].acquire() as conn:
        resx = await conn.execute(downstream.select()
                .where((downstream.c.annotation_id == annotation_id) &
                       (downstream.c.input_data_type == input_data_type) &
                       (downstream.c.analysis_type == "correlation")
                )
            )
        rowx = await resx.fetchone()
        if rowx:
            # 删除原本的分群结果目录
            rmtree(str(rowx["result_path"]))
            # 更新数据库中的结果
            async with conn.begin() as transaction:
                await conn.execute(downstream.update()
                    .where((downstream.c.annotation_id == annotation_id) &
                        (downstream.c.input_data_type == input_data_type) &
                        (downstream.c.analysis_type == "correlation")
                    )
                    .values(
                        slurm_job_id = jobID,
                        slurm_job_state = "PENDING",
                        result_path = userDir.absolute().__str__(),
                    )
                )
                await transaction.commit()
        else:
            async with conn.begin() as transaction:
                await conn.execute(downstream.insert().values(
                    user_id = int(data["user_id"]),
                    annotation_id = annotation_id,
                    input_data_type = input_data_type,
                    slurm_job_id = jobID,
                    slurm_job_state = "PENDING",
                    analysis_type = "correlation",
                    result_path = userDir.absolute().__str__(),
                ))
                await transaction.commit()

        return web.Response(text="OK")

async def scRNA_cell_cycle_score(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    load_data_r_code = ''
    qc_r_code = ''
    combined_r_code = ''
    dc_r_code = ''
    anno_type_r_code = ''
    anno_subtype_r_code = ''
    cell_cycle_r_code = ''
    
    # 1. 
    async with request.app['mysql_db'].acquire() as conn:
        sql = '''
            SELECT a.r_code, b.r_code
            FROM annotation_type a, upstream_dc b
            WHERE a.upstream_dc_id = b.id AND a.id = $anno_cluster_id;
        '''
        sql = sql.replace("$anno_cluster_id", str(data["anno_cluster_id"]))
        res = await conn.execute(sql)
        row = await res.fetchone()
        anno_type_r_code = row[0]
        dc_r_code = row[1]

        if str(data["anno_subcluster_id"]) != '-':
            res1 = await conn.execute(annotation_subtype.select()
                .where(annotation_subtype.c.id == int(data["anno_subcluster_id"]))
            )
            row1 = await res1.fetchone()
            anno_subtype_r_code = row1["dc_r_code"] + '\n' + row1["anno_r_code"] + '\n'

        if str(data["combined_id"]) != '-':
            # get combined_r_code
            if str(data["combined_type"]) == 'merge':
                res2 = await conn.execute(upstream_merge.select()
                    .where(upstream_merge.c.id == int(data["combined_id"]))
                )
                row2 = await res2.fetchone()
                combined_r_code = row2["r_code"]
            else:
                res2 = await conn.execute(upstream_integrate.select()
                    .where(upstream_integrate.c.id == int(data["combined_id"]))
                )
                row2 = await res2.fetchone()
                combined_r_code = row2["r_code"]

            # get qc_r_code load_data_r_code
            # merge/integrate data are not generated from generate_loom result
            for x in str(data["combined_data_ids"]).split(':'):
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(x)))
                row3 = await res3.fetchone()
                rbaseName = 'seurat_' + x

                if row3["data_type"] == "fastq":
                    res4 = await conn.execute(upstream_reads_mapping.select()
                        .where(upstream_reads_mapping.c.upload_data_id == row3["id"])
                    )
                    row4 = await res4.fetchone()
                    load_data_r_code += rbaseName+'.data <- Read10X("'+row4["result_path"]+'")\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res5 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.input_data_id == row4["id"])
                    )
                    row5 = await res5.fetchone()
                    qc_r_code += row5["r_code"]+"\n"

                elif row3["data_type"] == "cellranger":
                    load_data_r_code += rbaseName+'.data <- Read10X("'+row3["upload_path"]+'")\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res4 = await conn.execute(upstream_quality_control.select()
                            .where(upstream_quality_control.c.input_data_id == row3["id"])
                        )
                    row4 = await res4.fetchone()
                    qc_r_code += row4["r_code"] + "\n"

                else: # row3["data_type"] == "matrix"
                    load_data_r_code = rbaseName+'.data <- read.table(file="'+row3["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res4 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.input_data_id == row3["id"])
                    )
                    row4 = await res4.fetchone()
                    qc_r_code += row4["r_code"] + "\n"

        else: # str(data["combined_id"]) == '-':
            # get qc_r_code
            res2 = await conn.execute(upstream_quality_control.select()
                .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
            )
            row2 = await res2.fetchone()
            qc_r_code = row2["r_code"]
            # get load_data_r_code
            rbaseName = 'seurat_' + str(data["data_id"])
            if str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) == "-":
                res3 = await conn.execute(upstream_reads_mapping.select().
                        where(upstream_reads_mapping.c.upload_data_id == int(data["reads_mapping_id"]))
                    )
                row3 = await res3.fetchone()
                load_data_r_code = rbaseName+'.data <- Read10X("'+row3["result_path"]+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                
            elif str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) != "-":
                res3 = await conn.execute(upstream_generate_loom.select()
                        .where(upstream_generate_loom.c.id == int(data["generate_loom_id"]))
                    )
                row3 = await res3.fetchone()
                loom_file = row3['result_path'] + '/cellranger.loom'
                load_data_r_code = rbaseName+' <- read.loom.matrices(file = "'+loom_file+'")\n'+\
                    rbaseName+' <- as.Seurat('+rbaseName+')\n'+\
                    rbaseName+'[["RNA"]] <- '+rbaseName+'[["spliced"]]\n'+\
                    'DefaultAssay('+rbaseName+') <- "RNA"\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

            elif str(data["data_type"]) == 'cellranger':
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                row3 = await res3.fetchone()
                load_data_r_code = rbaseName+'.data <- Read10X("'+row3["upload_path"]+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

            elif str(data["data_type"]) == 'matrix':
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                row3 = await res3.fetchone()
                load_data_r_code = rbaseName+'.data <- read.table(file="'+row3["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
            else:
                pass
    
    # 3. generate cellcycleScore r code
    if str(data["anno_subcluster_id"]) == '-':
        if str(data["combined_id"]) != '-':
            cell_cycle_r_code = 'project.combined <- CellCycleScoring(object = project.combined, s.features = cc.genes$s.genes, g2m.features = cc.genes$g2m.genes, set.ident = F)\n'+\
            'project.combined@meta.data$G1_phase <- ifelse(project.combined@meta.data$Phase=="G1",1,0)\n'+\
            'project.combined@meta.data$S_phase <- ifelse(project.combined@meta.data$Phase=="S",1,0)\n'+\
            'project.combined@meta.data$M_phase <- ifelse(project.combined@meta.data$Phase=="G2M",1,0)\n'+\
            'p <- FeaturePlot(object = project.combined, features = c("S_phase", "M_phase"), blend=T)\n'+\
            'ggsave(filename = "cellcycle.png", plot = p, width = 32, height = 8, dpi=600, limitsize = F)'
        else:
            cell_cycle_r_code = rbaseName+' <- CellCycleScoring(object = '+rbaseName+', s.features = cc.genes$s.genes, g2m.features = cc.genes$g2m.genes, set.ident = F)\n'+\
            rbaseName+'@meta.data$G1_phase <- ifelse('+rbaseName+'@meta.data$Phase=="G1",1,0)\n'+\
            rbaseName+'@meta.data$S_phase <- ifelse('+rbaseName+'@meta.data$Phase=="S",1,0)\n'+\
            rbaseName+'@meta.data$M_phase <- ifelse('+rbaseName+'@meta.data$Phase=="G2M",1,0)\n'+\
            'p <- FeaturePlot(object = '+rbaseName+', features = c("S_phase", "M_phase"), blend=T)\n'+\
            'ggsave(filename = "cellcycle.png", plot = p, width = 32, height = 8, dpi=600, limitsize = F)\n'
    else:# data["anno_type"] == 'subcluster_annotation'
            cell_cycle_r_code = 'subcluster <- CellCycleScoring(object = subcluster, s.features = cc.genes$s.genes, g2m.features = cc.genes$g2m.genes, set.ident = F)\n'+\
            'subcluster@meta.data$G1_phase <- ifelse(subcluster@meta.data$Phase=="G1",1,0)\n'+\
            'subcluster@meta.data$S_phase <- ifelse(subcluster@meta.data$Phase=="S",1,0)\n'+\
            'subcluster@meta.data$M_phase <- ifelse(subcluster@meta.data$Phase=="G2M",1,0)\n'+\
            'p <- FeaturePlot(object = subcluster, features = c("S_phase", "M_phase"), blend=T)\n'+\
            'ggsave(filename = "cellcycle.png", plot = p, width = 32, height = 8, dpi=600, limitsize = F)\n'

    # 4. generate script file and submit slurm job
    script = r_script_cell_cycle.replace("#loadDataRcode", load_data_r_code)
    script = script.replace("#qcRcode", qc_r_code)
    if str(data["combined_id"]) != '-':
        script = script.replace("#combinedRcode", combined_r_code)
    script = script.replace("#dcRcode", dc_r_code)
    script = script.replace("#annotationTypeRcode", anno_type_r_code)
    if str(data["anno_subcluster_id"]) != '-':
        script = script.replace("#annotationSubtypeRcode", anno_subtype_r_code)
    script = script.replace("#cellCycleRcode", cell_cycle_r_code)

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="scRNA_correlation",
        suffix=".R",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file:
        await file.write(script)

    baseDir = pathlib.Path(config["cellCycleResultPath"])
    userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    while userDir.exists():
        userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    await asyncio.to_thread(userDir.mkdir, parents=True)
    
    script1 = downstream_bash.replace("partition", config["slurmPartition"])
    script1 = script1.replace("jobLogPath", config["slurmJobLogPath"])
    script1 = script1.replace("downstreamCode", "/usr/bin/Rscript " + file.name)
    script1 = script1.replace("userDir", userDir.absolute().__str__())
    script1 = script1.replace("$localcores", '1')

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode='w',
        prefix="correlation",
        suffix=".sh",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file1:
        await file1.write(script1)

    """
    cmd = "sbatch "+file1.name
    proc = await asyncio.create_subprocess_shell(
        cmd, 
        stdout=asyncio.subprocess.PIPE, 
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    x = stdout.decode().split()
    jobID = int(x[-1])
    """
    result = await asyncio.to_thread(subprocess.check_output, ['/usr/local/bin/sbatch', str(file1.name)]) 
    output = result.decode('utf-8')
    x = output.split()
    jobID = int(x[-1])
    
    # 5. insert value to downstream table
    async with lock:
        slurm_jobs[jobID] = 'cellcycle'

    if str(data["anno_subcluster_id"]) != "-":
        input_data_type = "annotation_subtype"
        annotation_id = int(data["anno_subcluster_id"])
    else:
        input_data_type = "annotation_type"
        annotation_id = int(data["anno_cluster_id"])

    async with request.app['mysql_db'].acquire() as conn:
        resx = await conn.execute(downstream.select()
                .where((downstream.c.annotation_id == annotation_id) &
                       (downstream.c.input_data_type == input_data_type) &
                       (downstream.c.analysis_type == "cellcycle")
                )
            )
        rowx = await resx.fetchone()
        if rowx:
            # 删除原本的分群结果目录
            rmtree(str(rowx["result_path"]))
            # 更新数据库中的结果
            async with conn.begin() as transaction:
                await conn.execute(downstream.update()
                    .where((downstream.c.annotation_id == annotation_id) &
                        (downstream.c.input_data_type == input_data_type) &
                        (downstream.c.analysis_type == "cellcycle")
                    )
                    .values(
                        slurm_job_id = jobID,
                        slurm_job_state = "PENDING",
                        result_path = userDir.absolute().__str__(),
                    )
                )
                await transaction.commit()
        else:
            async with conn.begin() as transaction:
                await conn.execute(downstream.insert().values(
                    user_id = int(data["user_id"]),
                    annotation_id = annotation_id,
                    input_data_type = input_data_type,
                    slurm_job_id = jobID,
                    slurm_job_state = "PENDING",
                    analysis_type = "cellcycle",
                    result_path = userDir.absolute().__str__(),
                ))
                await transaction.commit()

        return web.Response(text="OK")

async def scRNA_cell_frequency(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    load_data_r_code = ''
    qc_r_code = ''
    combined_r_code = ''
    dc_r_code = ''
    anno_type_r_code = ''
    anno_subtype_r_code = ''
    cell_frequency_r_code = ''

    # 1. 
    async with request.app['mysql_db'].acquire() as conn:
        sql = '''
            SELECT a.r_code, b.r_code
            FROM annotation_type a, upstream_dc b
            WHERE a.upstream_dc_id = b.id AND a.id = $anno_cluster_id;
        '''
        sql = sql.replace("$anno_cluster_id", str(data["anno_cluster_id"]))
        res = await conn.execute(sql)
        row = await res.fetchone()
        anno_type_r_code = row[0]
        dc_r_code = row[1]

        if str(data["anno_subcluster_id"]) != '-':
            res1 = await conn.execute(annotation_subtype.select()
                .where(annotation_subtype.c.id == int(data["anno_subcluster_id"]))
            )
            row1 = await res1.fetchone()
            anno_subtype_r_code = row1["dc_r_code"] + '\n' + row1["anno_r_code"] + '\n'

        if str(data["combined_id"]) != '-':
            # get combined_r_code
            if str(data["combined_type"]) == 'merge':
                res2 = await conn.execute(upstream_merge.select()
                    .where(upstream_merge.c.id == int(data["combined_id"]))
                )
                row2 = await res2.fetchone()
                combined_r_code = row2["r_code"]
            else:
                res2 = await conn.execute(upstream_integrate.select()
                    .where(upstream_integrate.c.id == int(data["combined_id"]))
                )
                row2 = await res2.fetchone()
                combined_r_code = row2["r_code"]

            # get qc_r_code load_data_r_code
            # merge/integrate data are not generated from generate_loom result
            for x in str(data["combined_data_ids"]).split(':'):
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(x)))
                row3 = await res3.fetchone()
                rbaseName = 'seurat_' + x

                if row3["data_type"] == "fastq":
                    res4 = await conn.execute(upstream_reads_mapping.select()
                        .where(upstream_reads_mapping.c.upload_data_id == row3["id"])
                    )
                    row4 = await res4.fetchone()
                    load_data_r_code += rbaseName+'.data <- Read10X("'+row4["result_path"]+'")\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res5 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.input_data_id == row4["id"])
                    )
                    row5 = await res5.fetchone()
                    qc_r_code += row5["r_code"]+"\n"

                elif row3["data_type"] == "cellranger":
                    load_data_r_code += rbaseName+'.data <- Read10X("'+row3["upload_path"]+'")\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res4 = await conn.execute(upstream_quality_control.select()
                            .where(upstream_quality_control.c.input_data_id == row3["id"])
                        )
                    row4 = await res4.fetchone()
                    qc_r_code += row4["r_code"] + "\n"

                else: # row3["data_type"] == "matrix"
                    load_data_r_code = rbaseName+'.data <- read.table(file="'+row3["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res4 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.input_data_id == row3["id"])
                    )
                    row4 = await res4.fetchone()
                    qc_r_code += row4["r_code"] + "\n"

        else: # str(data["combined_id"]) == '-':
            # get qc_r_code
            res2 = await conn.execute(upstream_quality_control.select()
                .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
            )
            row2 = await res2.fetchone()
            qc_r_code = row2["r_code"]
            # get load_data_r_code
            rbaseName = 'seurat_' + str(data["data_id"])
            if str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) == "-":
                res3 = await conn.execute(upstream_reads_mapping.select().
                        where(upstream_reads_mapping.c.upload_data_id == int(data["reads_mapping_id"]))
                    )
                row3 = await res3.fetchone()
                load_data_r_code = rbaseName+'.data <- Read10X("'+row3["result_path"]+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                
            elif str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) != "-":
                res3 = await conn.execute(upstream_generate_loom.select()
                        .where(upstream_generate_loom.c.id == int(data["generate_loom_id"]))
                    )
                row3 = await res3.fetchone()
                loom_file = row3['result_path'] + '/cellranger.loom'
                load_data_r_code = rbaseName+' <- read.loom.matrices(file = "'+loom_file+'")\n'+\
                    rbaseName+' <- as.Seurat('+rbaseName+')\n'+\
                    rbaseName+'[["RNA"]] <- '+rbaseName+'[["spliced"]]\n'+\
                    'DefaultAssay('+rbaseName+') <- "RNA"\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

            elif str(data["data_type"]) == 'cellranger':
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                row3 = await res3.fetchone()
                load_data_r_code = rbaseName+'.data <- Read10X("'+row3["upload_path"]+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

            elif str(data["data_type"]) == 'matrix':
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                row3 = await res3.fetchone()
                load_data_r_code = rbaseName+'.data <- read.table(file="'+row3["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
            else:
                pass
  
    # 3. generate cell_frequency_r_code
    if str(data["anno_subcluster_id"]) == '-':
        if str(data["combined_id"]) != "-":
            cell_frequency_r_code = 'cell.prop <- data.frame(table(project.combined$check_clusters))\n'+\
            '''
for (i in 1:nrow(cell.prop)) {cell.prop$percent[i] <- cell.prop[i,]$Freq/sum(cell.prop$Freq)*100}
colnames(cell.prop) <- c("Celltype", "Freq", "Percentage")
cell.prop <- arrange(cell.prop, desc(Percentage))
cell.prop$Celltype <- factor(cell.prop$Celltype, levels = cell.prop$Celltype)
p <- ggplot(cell.prop,aes(Celltype,Percentage, fill = Celltype))+
  geom_bar(stat="identity", position="dodge")+ggtitle("")+
  theme_bw()+theme(axis.ticks.length=unit(0.5,'cm'))+
  guides(fill=guide_legend(title=NULL))+theme_classic()+
  theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1),axis.ticks.length=unit(0.2,'cm'),text = element_text(size = 15),axis.line = element_line(colour = 'black'),axis.text = element_text(colour = 'black'))+
  labs(x = NULL, y = 'Percentage') + NoLegend()
ggsave(filename = 'cellproportion.png', plot = p, width = 15, height = 12, dpi = 600)

            '''

        else:
            cell_frequency_r_code = 'cell.prop <- data.frame(table('+rbaseName+'$check_clusters))\n'+\
            '''
for (i in 1:nrow(cell.prop)) {cell.prop$percent[i] <- cell.prop[i,]$Freq/sum(cell.prop$Freq)*100}
colnames(cell.prop) <- c("Celltype", "Freq", "Percentage")
cell.prop <- arrange(cell.prop, desc(Percentage))
cell.prop$Celltype <- factor(cell.prop$Celltype, levels = cell.prop$Celltype)
p <- ggplot(cell.prop,aes(Celltype,Percentage, fill = Celltype))+
  geom_bar(stat="identity", position="dodge")+ggtitle("")+
  theme_bw()+theme(axis.ticks.length=unit(0.5,'cm'))+
  guides(fill=guide_legend(title=NULL))+theme_classic()+
  theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1),axis.ticks.length=unit(0.2,'cm'),text = element_text(size = 15),axis.line = element_line(colour = 'black'),axis.text = element_text(colour = 'black'))+
  labs(x = NULL, y = 'Percentage') + NoLegend()
ggsave(filename = './cellproportion.png', plot = p, width = 15, height = 12, dpi = 600)

            '''
    else:
            cell_frequency_r_code = 'cell.prop <- data.frame(table(subcluster$check_clusters))\n'+\
            '''
for (i in 1:nrow(cell.prop)) {cell.prop$percent[i] <- cell.prop[i,]$Freq/sum(cell.prop$Freq)*100}
colnames(cell.prop) <- c("Celltype", "Freq", "Percentage")
cell.prop <- arrange(cell.prop, desc(Percentage))
cell.prop$Celltype <- factor(cell.prop$Celltype, levels = cell.prop$Celltype)
p <- ggplot(cell.prop,aes(Celltype,Percentage, fill = Celltype))+
  geom_bar(stat="identity", position="dodge")+ggtitle("")+
  theme_bw()+theme(axis.ticks.length=unit(0.5,'cm'))+
  guides(fill=guide_legend(title=NULL))+theme_classic()+
  theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1),axis.ticks.length=unit(0.2,'cm'),text = element_text(size = 15),axis.line = element_line(colour = 'black'),axis.text = element_text(colour = 'black'))+
  labs(x = NULL, y = 'Percentage') + NoLegend()
ggsave(filename = './cellproportion.png', plot = p, width = 15, height = 12, dpi = 600)
            '''

    # 4. generate script file and submit slurm job
    script = r_script_cell_frequency.replace("#loadDataRcode", load_data_r_code)
    script = script.replace("#qcRcode", qc_r_code)
    if str(data["combined_id"]) != "-":
        script = script.replace("#combinedRcode", combined_r_code)
    script = script.replace("#dcRcode", dc_r_code)
    script = script.replace("#annotationTypeRcode", anno_type_r_code)
    if str(data["anno_subcluster_id"]) != '-':
        script = script.replace("#annotationSubtypeRcode", anno_subtype_r_code)
    script = script.replace("#cellFrequencyRcode", cell_frequency_r_code)

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="cellfrequency",
        suffix=".R",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file:
        await file.write(script)

    baseDir = pathlib.Path(config["cellFrequencyResultPath"])
    userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    while userDir.exists():
        userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    await asyncio.to_thread(userDir.mkdir, parents=True)
    
    script1 = downstream_bash.replace("partition", config["slurmPartition"])
    script1 = script1.replace("jobLogPath", config["slurmJobLogPath"])
    script1 = script1.replace("downstreamCode", "/usr/bin/Rscript " + file.name)
    script1 = script1.replace("userDir", userDir.absolute().__str__())
    script1 = script1.replace("$localcores", '1')

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode='w',
        prefix="cell_frequency",
        suffix=".sh",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file1:
        await file1.write(script1)

    """
    cmd = "sbatch "+file1.name
    proc = await asyncio.create_subprocess_shell(
        cmd, 
        stdout=asyncio.subprocess.PIPE, 
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    x = stdout.decode().split()
    jobID = int(x[-1])
    """
    result = await asyncio.to_thread(subprocess.check_output, ['/usr/local/bin/sbatch', str(file1.name)]) 
    output = result.decode('utf-8')
    x = output.split()
    jobID = int(x[-1])

    # 5. insert value to downstream table
    async with lock:
        slurm_jobs[jobID] = 'cell_freqeuncy'

    if str(data["anno_subcluster_id"]) != "-":
        input_data_type = "annotation_subtype"
        annotation_id = int(data["anno_subcluster_id"])
    else:
        input_data_type = "annotation_type"
        annotation_id = int(data["anno_cluster_id"])

    async with request.app['mysql_db'].acquire() as conn:
        resx = await conn.execute(downstream.select()
                .where((downstream.c.annotation_id == annotation_id) &
                       (downstream.c.input_data_type == input_data_type) &
                       (downstream.c.analysis_type == "cell_frequency")
                )
            )
        rowx = await resx.fetchone()
        if rowx:
            # 删除原本的分群结果目录
            rmtree(str(rowx["result_path"]))
            # 更新数据库中的结果
            async with conn.begin() as transaction:
                await conn.execute(downstream.update()
                    .where((downstream.c.annotation_id == annotation_id) &
                        (downstream.c.input_data_type == input_data_type) &
                        (downstream.c.analysis_type == "cell_frequency")
                    )
                    .values(
                        slurm_job_id = jobID,
                        slurm_job_state = "PENDING",
                        result_path = userDir.absolute().__str__(),
                    )
                )
                await transaction.commit()
        else:
            async with conn.begin() as transaction:
                await conn.execute(downstream.insert().values(
                    user_id = int(data["user_id"]),
                    annotation_id = annotation_id,
                    input_data_type = input_data_type,
                    slurm_job_id = jobID,
                    slurm_job_state = "PENDING",
                    analysis_type = "cell_frequency",
                    result_path = userDir.absolute().__str__(),
                ))
                await transaction.commit()

        return web.Response(text="OK")

# solve clusterProfile erichKEGG function error
# https://github.com/YuLab-SMU/clusterProfiler/issues/561#issuecomment-1467266614
async def scRNA_enrichment(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    load_data_r_code = ''
    qc_r_code = ''
    combined_r_code = ''
    dc_r_code = ''
    anno_type_r_code = ''
    anno_subtype_r_code = ''
    enrichment_r_code = ''

    right_species = ''

    # 1. 
    async with request.app['mysql_db'].acquire() as conn:
        sql = '''
            SELECT a.r_code, b.r_code
            FROM annotation_type a, upstream_dc b
            WHERE a.upstream_dc_id = b.id AND a.id = $anno_cluster_id;
        '''
        sql = sql.replace("$anno_cluster_id", str(data["anno_cluster_id"]))
        res = await conn.execute(sql)
        row = await res.fetchone()
        anno_type_r_code = row[0]
        dc_r_code = row[1]

        if str(data["anno_subcluster_id"]) != '-':
            res1 = await conn.execute(annotation_subtype.select()
                .where(annotation_subtype.c.id == int(data["anno_subcluster_id"]))
            )
            row1 = await res1.fetchone()
            anno_subtype_r_code = row1["dc_r_code"] + '\n' + row1["anno_r_code"] + '\n'

        if str(data["combined_id"]) != '-':
            # get combined_r_code
            if str(data["combined_type"]) == 'merge':
                res2 = await conn.execute(upstream_merge.select()
                    .where(upstream_merge.c.id == int(data["combined_id"]))
                )
                row2 = await res2.fetchone()
                combined_r_code = row2["r_code"]
            else:
                res2 = await conn.execute(upstream_integrate.select()
                    .where(upstream_integrate.c.id == int(data["combined_id"]))
                )
                row2 = await res2.fetchone()
                combined_r_code = row2["r_code"]

            # get qc_r_code load_data_r_code
            # merge/integrate data are not generated from generate_loom result
            for x in str(data["combined_data_ids"]).split(':'):
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(x)))
                row3 = await res3.fetchone()
                rbaseName = 'seurat_' + x
                right_species = row3["species"]

                if row3["data_type"] == "fastq":
                    res4 = await conn.execute(upstream_reads_mapping.select()
                        .where(upstream_reads_mapping.c.upload_data_id == row3["id"])
                    )
                    row4 = await res4.fetchone()
                    load_data_r_code += rbaseName+'.data <- Read10X("'+row4["result_path"]+'")\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res5 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.input_data_id == row4["id"])
                    )
                    row5 = await res5.fetchone()
                    qc_r_code += row5["r_code"]+"\n"

                elif row3["data_type"] == "cellranger":
                    load_data_r_code += rbaseName+'.data <- Read10X("'+row3["upload_path"]+'")\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res4 = await conn.execute(upstream_quality_control.select()
                            .where(upstream_quality_control.c.input_data_id == row3["id"])
                        )
                    row4 = await res4.fetchone()
                    qc_r_code += row4["r_code"] + "\n"

                else: # row3["data_type"] == "matrix"
                    load_data_r_code = rbaseName+'.data <- read.table(file="'+row3["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res4 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.input_data_id == row3["id"])
                    )
                    row4 = await res4.fetchone()
                    qc_r_code += row4["r_code"] + "\n"

        else: # str(data["combined_id"]) == '-':
            # get qc_r_code
            res2 = await conn.execute(upstream_quality_control.select()
                .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
            )
            row2 = await res2.fetchone()
            qc_r_code = row2["r_code"]
            # get load_data_r_code
            rbaseName = 'seurat_' + str(data["data_id"])
            if str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) == "-":
                resxx = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                rowxx = await resxx.fetchone()
                right_species = rowxx["species"]

                res3 = await conn.execute(upstream_reads_mapping.select().
                        where(upstream_reads_mapping.c.upload_data_id == int(data["reads_mapping_id"]))
                    )
                row3 = await res3.fetchone()
                load_data_r_code = rbaseName+'.data <- Read10X("'+row3["result_path"]+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                
            elif str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) != "-":
                resxx = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                rowxx = await resxx.fetchone()
                right_species = rowxx["species"]

                res3 = await conn.execute(upstream_generate_loom.select()
                        .where(upstream_generate_loom.c.id == int(data["generate_loom_id"]))
                    )
                row3 = await res3.fetchone()
                loom_file = row3['result_path'] + '/cellranger.loom'
                load_data_r_code = rbaseName+' <- read.loom.matrices(file = "'+loom_file+'")\n'+\
                    rbaseName+' <- as.Seurat('+rbaseName+')\n'+\
                    rbaseName+'[["RNA"]] <- '+rbaseName+'[["spliced"]]\n'+\
                    'DefaultAssay('+rbaseName+') <- "RNA"\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

            elif str(data["data_type"]) == 'cellranger':
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                row3 = await res3.fetchone()
                right_species = row3["species"]
                load_data_r_code = rbaseName+'.data <- Read10X("'+row3["upload_path"]+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

            elif str(data["data_type"]) == 'matrix':
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                row3 = await res3.fetchone()
                right_species = row3["species"]
                load_data_r_code = rbaseName+'.data <- read.table(file="'+row3["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
            else:
                pass
  
    # 3. generate enrichment_r_code
    if str(data["anno_subcluster_id"]) == '-':
        if str(data["combined_id"]) != "-":
            if data["analysis_type"] == "two_group": # 
                enrichment_r_code = 'Idents(project.combined) <- "check_clusters"\n'+\
                'target.celltype <- subset(project.combined, idents = "'+str(data["target_celltype"])+'")\n'+\
                'Idents(target.celltype) <- "type"\n'+\
                'markers <- FindMarkers(target.celltype, ident.1 = "'+str(data["group_one"])+'", ident.2 = "'+str(data["group_two"])+'", only.pos = T)\n'
            else: # data["analysis_type"] == "two_celltype"
                enrichment_r_code = 'Idents(project.combined) <- "check_clusters"\n'+\
                'markers <- FindMarkers(project.combined, ident.1 = "'+str(data["celltype_one"])+'", ident.2 = "'+str(data["celltype_two"])+'", only.pos = T)\n'
        else: # str(data["combined_id"]) == "-"
            if data["analysis_type"] == "two_group": # has no type meta data in seurat object
                pass
            else: # data["analysis_type"] == "two_celltype"
                enrichment_r_code = 'Idents('+rbaseName+') <- "check_clusters"\n'+\
                'markers <- FindMarkers('+rbaseName+', ident.1 = "'+str(data["celltype_one"])+'", ident.2 = "'+str(data["celltype_two"])+'", only.pos = T)\n'
    else:# str(data["anno_subcluster_id"]) != '-'
        if data["analysis_type"] == "two_group": # has potential bug, if subscluster come from no-merge/integrate data
            enrichment_r_code = 'Idents(subcluster) <- "check_clusters"\n'+\
            'target.celltype <- subset(subcluster, idents = "'+str(data["target_celltype"])+'")\n'+\
            'Idents(target.celltype) <- "type"\n'+\
            'markers <- FindMarkers(target.celltype, ident.1 = "'+str(data["group_one"])+'", ident.2 = "'+str(data["group_two"])+'", only.pos = T)\n'
        else: # data["analysis_type"] == "two_celltype"
            enrichment_r_code = 'Idents(subcluster) <- "check_clusters"\n'+\
            'markers <- FindMarkers(subcluster, ident.1 = "'+str(data["celltype_one"])+'", ident.2 = "'+str(data["celltype_two"])+'", only.pos = T)\n'

    enrichment_r_code += r'''
markers <- markers %>% filter(p_val_adj < 0.05)
marker_genes <- rownames(markers)
geneID_tmp <-  bitr(marker_genes, fromType = "SYMBOL",toType = c("ENTREZID"), OrgDb = OrgDb)
KEGG <- enrichKEGG(gene = geneID_tmp$ENTREZID, organism = organism, keyType = "kegg", use_internal_data = T)
p <- barplot(KEGG, showCategory = 15,title = "")
ggsave(filename = paste0("KEGG_.png"), plot = p, width = 8, height = 7,dpi = 600)
CC <- enrichGO(geneID_tmp$ENTREZID, OrgDb = OrgDb, ont = "CC")
BP <- enrichGO(geneID_tmp$ENTREZID, OrgDb = OrgDb, ont = "BP")
MF <- enrichGO(geneID_tmp$ENTREZID, OrgDb = OrgDb, ont = "MF")
p1 <- barplot(CC, showCategory = 15,title = "")
p2 <- barplot(MF, showCategory = 15,title = "")
p3 <- barplot(BP, showCategory = 15,title = "")
ggsave(filename = "CC_.png", plot = p1, width = 8, height = 7,dpi = 600)
ggsave(filename = "BP_.png", plot = p2, width = 8, height = 7,dpi = 600)
ggsave(filename = "MF_.png", plot = p3, width = 8, height = 7,dpi = 600)
'''
    # 4. generate script file and submit slurm job 
    script = r_script_enrichment.replace("#loadDataRcode", load_data_r_code)
    script = script.replace("data_species", right_species)
    script = script.replace("#qcRcode", qc_r_code)
    if str(data["combined_id"])!="-":
        script = script.replace("#combinedRcode", combined_r_code)
    script = script.replace("#dcRcode", dc_r_code)
    script = script.replace("#annotationTypeRcode", anno_type_r_code)
    if str(data["anno_subcluster_id"]) != '-':
        script = script.replace("#annotationSubtypeRcode", anno_subtype_r_code)
    script = script.replace("#enrichmentRcode", enrichment_r_code)

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w", 
        prefix="enrichment",
        suffix=".R",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file:
        await file.write(script)

    baseDir = pathlib.Path(config["enrichmentResultPath"])
    userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    while userDir.exists():
        userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    await asyncio.to_thread(userDir.mkdir, parents=True)
    
    script1 = downstream_bash.replace("partition", config["slurmPartition"])
    script1 = script1.replace("jobLogPath", config["slurmJobLogPath"])
    script1 = script1.replace("downstreamCode", "/usr/bin/Rscript " + file.name)
    script1 = script1.replace("userDir", userDir.absolute().__str__())
    script1 = script1.replace("$localcores", '1')

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="enrichment",
        suffix=".sh",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file1:
        await file1.write(script1)

    """cmd = "sbatch "+file1.name
    proc = await asyncio.create_subprocess_shell(
        cmd, 
        stdout=asyncio.subprocess.PIPE, 
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    x = stdout.decode().split()
    jobID = int(x[-1])
    """

    result = await asyncio.to_thread(subprocess.check_output, ['/usr/local/bin/sbatch', str(file1.name)]) 
    output = result.decode('utf-8')
    x = output.split()
    jobID = int(x[-1])
    
    # 5. insert value to downstream table
    async with lock:
        slurm_jobs[jobID] = 'enrichment'

    if str(data["anno_subcluster_id"]) != "-":
        input_data_type = "annotation_subtype"
        annotation_id = int(data["anno_subcluster_id"])
    else:
        input_data_type = "annotation_type"
        annotation_id = int(data["anno_cluster_id"])

    async with request.app['mysql_db'].acquire() as conn:
        resx = await conn.execute(downstream.select()
                .where((downstream.c.annotation_id == annotation_id) &
                       (downstream.c.input_data_type == input_data_type) &
                       (downstream.c.analysis_type == "enrichment")
                )
            )
        rowx = await resx.fetchone()
        if rowx:
            # 删除原本的分群结果目录
            rmtree(str(rowx["result_path"]))
            # 更新数据库中的结果
            async with conn.begin() as transaction:
                await conn.execute(downstream.update()
                    .where((downstream.c.annotation_id == annotation_id) &
                        (downstream.c.input_data_type == input_data_type) &
                        (downstream.c.analysis_type == "enrichment")
                    )
                    .values(
                        slurm_job_id = jobID,
                        slurm_job_state = "PENDING",
                        result_path = userDir.absolute().__str__(),
                    )
                )
                await transaction.commit()
        else:
            async with conn.begin() as transaction:
                await conn.execute(downstream.insert().values(
                    user_id = int(data["user_id"]),
                    annotation_id = annotation_id,
                    input_data_type = input_data_type,
                    slurm_job_id = jobID,
                    slurm_job_state = "PENDING",
                    analysis_type = "enrichment",
                    result_path = userDir.absolute().__str__(),
                ))
                await transaction.commit()


        return web.Response(text="OK")

async def scRNA_GSEA(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    load_data_r_code = ''
    qc_r_code = ''
    combined_r_code = ''
    dc_r_code = ''
    anno_type_r_code = ''
    anno_subtype_r_code = ''
    gsea_sh_code = ''
    gsea_r_code = ''

    right_species = ''

    # 1. 
    async with request.app['mysql_db'].acquire() as conn:
        sql = '''
            SELECT a.r_code, b.r_code
            FROM annotation_type a, upstream_dc b
            WHERE a.upstream_dc_id = b.id AND a.id = $anno_cluster_id;
        '''
        sql = sql.replace("$anno_cluster_id", str(data["anno_cluster_id"]))
        res = await conn.execute(sql)
        row = await res.fetchone()
        anno_type_r_code = row[0]
        dc_r_code = row[1]

        if str(data["anno_subcluster_id"]) != '-':
            res1 = await conn.execute(annotation_subtype.select()
                .where(annotation_subtype.c.id == int(data["anno_subcluster_id"]))
            )
            row1 = await res1.fetchone()
            anno_subtype_r_code = row1["dc_r_code"] + '\n' + row1["anno_r_code"] + '\n'

        if str(data["combined_id"]) != '-':
            # get combined_r_code
            if str(data["combined_type"]) == 'merge':
                res2 = await conn.execute(upstream_merge.select()
                    .where(upstream_merge.c.id == int(data["combined_id"]))
                )
                row2 = await res2.fetchone()
                combined_r_code = row2["r_code"]
            else:
                res2 = await conn.execute(upstream_integrate.select()
                    .where(upstream_integrate.c.id == int(data["combined_id"]))
                )
                row2 = await res2.fetchone()
                combined_r_code = row2["r_code"]

            # get qc_r_code load_data_r_code
            # merge/integrate data are not generated from generate_loom result
            for x in str(data["combined_data_ids"]).split(':'):
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(x)))
                row3 = await res3.fetchone()
                rbaseName = 'seurat_' + x
                right_species = row3['species']

                if row3["data_type"] == "fastq":
                    res4 = await conn.execute(upstream_reads_mapping.select()
                        .where(upstream_reads_mapping.c.upload_data_id == row3["id"])
                    )
                    row4 = await res4.fetchone()
                    load_data_r_code += rbaseName+'.data <- Read10X("'+row4["result_path"]+'")\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res5 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.input_data_id == row4["id"])
                    )
                    row5 = await res5.fetchone()
                    qc_r_code += row5["r_code"]+"\n"

                elif row3["data_type"] == "cellranger":
                    load_data_r_code += rbaseName+'.data <- Read10X("'+row3["upload_path"]+'")\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res4 = await conn.execute(upstream_quality_control.select()
                            .where(upstream_quality_control.c.input_data_id == row3["id"])
                        )
                    row4 = await res4.fetchone()
                    qc_r_code += row4["r_code"] + "\n"

                else: # row3["data_type"] == "matrix"
                    load_data_r_code = rbaseName+'.data <- read.table(file="'+row3["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res4 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.input_data_id == row3["id"])
                    )
                    row4 = await res4.fetchone()
                    qc_r_code += row4["r_code"] + "\n"

        else: # str(data["combined_id"]) == '-':
            # get qc_r_code
            res2 = await conn.execute(upstream_quality_control.select()
                .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
            )
            row2 = await res2.fetchone()
            qc_r_code = row2["r_code"]
            # get load_data_r_code
            rbaseName = 'seurat_' + str(data["data_id"])
            if str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) == "-":
                resxx = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                rowxx = await resxx.fetchone()
                right_species = rowxx["species"]

                res3 = await conn.execute(upstream_reads_mapping.select().
                        where(upstream_reads_mapping.c.upload_data_id == int(data["reads_mapping_id"]))
                    )
                row3 = await res3.fetchone()
                load_data_r_code = rbaseName+'.data <- Read10X("'+row3["result_path"]+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                
            elif str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) != "-":
                resxx = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                rowxx = await resxx.fetchone()
                right_species = rowxx["species"]
            
                res3 = await conn.execute(upstream_generate_loom.select()
                        .where(upstream_generate_loom.c.id == int(data["generate_loom_id"]))
                    )
                row3 = await res3.fetchone()
                loom_file = row3['result_path'] + '/cellranger.loom'
                load_data_r_code = rbaseName+' <- read.loom.matrices(file = "'+loom_file+'")\n'+\
                    rbaseName+' <- as.Seurat('+rbaseName+')\n'+\
                    rbaseName+'[["RNA"]] <- '+rbaseName+'[["spliced"]]\n'+\
                    'DefaultAssay('+rbaseName+') <- "RNA"\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

            elif str(data["data_type"]) == 'cellranger':
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                row3 = await res3.fetchone()
                right_species = row3["species"]

                load_data_r_code = rbaseName+'.data <- Read10X("'+row3["upload_path"]+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

            elif str(data["data_type"]) == 'matrix':
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                row3 = await res3.fetchone()
                right_species = row3["species"]

                load_data_r_code = rbaseName+'.data <- read.table(file="'+row3["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
            else:
                pass
    
    # 3. generate corelation_r_code
    hsa_gene_set = {} # geneset gmt file name storage
    mmu_gene_set = {}
    # human geneset
    hsa_gene_set["C1"] = "c1.all.v2023.1.Hs.symbols.gmt"
    hsa_gene_set["C2"] = "c2.all.v2023.1.Hs.symbols.gmt"
    hsa_gene_set["C3"] = "c3.all.v2023.1.Hs.symbols.gmt"
    hsa_gene_set["C4"] = "c4.all.v2023.1.Hs.symbols.gmt"
    hsa_gene_set["C5"] = "c5.all.v2023.1.Hs.symbols.gmt"
    hsa_gene_set["C6"] = "c6.all.v2023.1.Hs.symbols.gmt"
    hsa_gene_set["C7"] = "c7.all.v2023.1.Hs.symbols.gmt"
    hsa_gene_set["C8"] = "c8.all.v2023.1.Hs.symbols.gmt"
    hsa_gene_set["H"] = "h.all.v2023.1.Hs.symbols.gmt"
    # mouse geneset
    mmu_gene_set["MH"] = "mh.all.v2023.1.Mm.symbols.gmt"
    mmu_gene_set["M1"] = "m1.all.v2023.1.Mm.symbols.gmt"
    mmu_gene_set["M2"] = "m2.all.v2023.1.Mm.symbols.gmt"
    mmu_gene_set["M3"] = "m3.all.v2023.1.Mm.symbols.gmt"
    mmu_gene_set["M5"] = "m5.all.v2023.1.Mm.symbols.gmt"
    mmu_gene_set["M8"] = "m8.all.v2023.1.Mm.symbols.gmt"

    if right_species == 'human' and data["gene_set"] in hsa_gene_set:
        gmt_file = str(config["geneSetGmtFilePath"]) + "/" + hsa_gene_set[data["gene_set"]]
        
    elif right_species == 'human' and data["gene_set"] not in hsa_gene_set:
        return web.Response(text="error: species: human, gene_set error")
    
    elif right_species == 'mouse' and data["gene_set"] in mmu_gene_set:
        gmt_file = str(config["geneSetGmtFilePath"]) + "/" + mmu_gene_set[data["gene_set"]]
        
    elif right_species == 'mouse' and data["gene_set"] not in mmu_gene_set:
        return web.Response(text="error: species: mouse, gene_set error")
    else:
        pass

    if str(data["combined_id"]) != "-":
        rbaseName = "project.combined"

    if str(data["anno_subcluster_id"]) == '-':
        gsea_r_code = 'Idents('+rbaseName+') <- "check_clusters"\n'+\
        'sub <- subset('+rbaseName+', idents = c("'+str(data["cluster_one"])+'", "'+str(data["cluster_two"])+'"))\n'+\
        r'''
epr <- GetAssayData(sub[["RNA"]], slot = 'counts')
epr <- data.frame(NAME=rownames(epr), Description=rep('na', nrow(epr)), epr, stringsAsFactors=F)
write('#1.2', "expr.gct", ncolumns=1)
write(c(nrow(epr),(ncol(epr)-2)), "expr.gct", ncolumns=2, append=T, sep='\t')
write.table(epr, "expr.gct", row.names=F, sep='\t', append=T, quote=F)
line.1 <- c((ncol(epr)-2), 2, 1)
tmp <- as.vector(sub@meta.data$check_clusters)
tmp <- gsub("\\s", "_", tmp)
line.2 <- c("#", unique(tmp))
write(line.1, 'group.cls', ncolumns=length(line.1), append=T, sep='\t')
write(line.2, 'group.cls', ncolumns=length(line.2), append=T, sep='\t')
write(tmp, 'group.cls', ncolumns=length(tmp), append=T, sep='\t')
        '''
    else:# str(data["anno_subcluster_id"]) != '-'
        gsea_r_code = 'Idents(subcluster) <- "check_clusters"\n'+\
        'sub <- subset(subcluster, idents = c("'+str(data["cluster_one"])+'", "'+str(data["cluster_two"])+'"))\n'+\
        r'''
epr <- GetAssayData(sub[["RNA"]], slot = 'counts')
epr <- data.frame(NAME=rownames(epr), Description=rep('na', nrow(epr)), epr, stringsAsFactors=F)
write('#1.2', "expr.gct", ncolumns=1)
write(c(nrow(epr),(ncol(epr)-2)), "expr.gct", ncolumns=2, append=T, sep='\t')
write.table(epr, "expr.gct", row.names=F, sep='\t', append=T, quote=F)
tmp <- as.vector(sub@meta.data$check_clusters)
tmp <- gsub("\\s", "_", tmp)
line.1 <- c((ncol(epr)-2), 2, 1)
line.2 <- c("#", unique(tmp))
write(line.1, 'group.cls', ncolumns=length(line.1), append=T, sep='\t')
write(line.2, 'group.cls', ncolumns=length(line.2), append=T, sep='\t')
write(tmp, 'group.cls', ncolumns=length(tmp), append=T, sep='\t')
        '''

    # 4. generate script file and submit slurm job 
    script = r_script_GSEA.replace("#loadDataRcode", load_data_r_code)
    script = script.replace("data_species", right_species)
    script = script.replace("#qcRcode", qc_r_code)
    if str(data["combined_id"]) != "-":
        script = script.replace("#combinedRcode}", combined_r_code)
    script = script.replace("#dcRcode", dc_r_code)
    script = script.replace("#annotationTypeRcode", anno_type_r_code)
    if str(data["anno_subcluster_id"]) != '-':
        script = script.replace("#annotationSubtypeRcode", anno_subtype_r_code)
    script = script.replace("#GSEARcode", gsea_r_code)
    
    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="gsea",
        suffix=".R",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file:
        await file.write(script)

    baseDir = pathlib.Path(config["gseaResultPath"])
    userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    while userDir.exists():
        userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    await asyncio.to_thread(userDir.mkdir, parents=True)

    script1 = downstream_bash.replace("partition", config["slurmPartition"])
    script1 = script1.replace("jobLogPath", config["slurmJobLogPath"])
    script1 = script1.replace("downstreamCode", "/usr/bin/Rscript " + file.name)
    script1 = script1.replace("userDir", userDir.absolute().__str__())
    script1 = script1.replace("$localcores", '1')
    gsea_sh_code = str(config["gseaSoftwarePath"]) + ' GSEA -collapse false -res expr.gct -cls group.cls -gmx '+gmt_file+' -out ./'
    script1 += '\n' + gsea_sh_code

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="gsea",
        suffix=".sh",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file1:
        await file1.write(script1)

    """
    cmd = "sbatch "+file1.name
    proc = await asyncio.create_subprocess_shell(
        cmd, 
        stdout=asyncio.subprocess.PIPE, 
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    x = stdout.decode().split()
    jobID = int(x[-1])
    """

    result = await asyncio.to_thread(subprocess.check_output, ['/usr/local/bin/sbatch', str(file1.name)]) 
    output = result.decode('utf-8')
    x = output.split()
    jobID = int(x[-1])
    
    # 5. insert value to downstream table
    async with lock:
        slurm_jobs[jobID] = 'gsea'

    if str(data["anno_subcluster_id"]) != "-":
        input_data_type = "annotation_subtype"
        annotation_id = int(data["anno_subcluster_id"])
    else:
        input_data_type = "annotation_type"
        annotation_id = int(data["anno_cluster_id"])

    async with request.app['mysql_db'].acquire() as conn:
        resx = await conn.execute(downstream.select()
                .where((downstream.c.annotation_id == annotation_id) &
                       (downstream.c.input_data_type == input_data_type) &
                       (downstream.c.analysis_type == "gsea")
                )
            )
        rowx = await resx.fetchone()
        if rowx:
            # 删除原本的分群结果目录
            rmtree(str(rowx["result_path"]))
            # 更新数据库中的结果
            async with conn.begin() as transaction:
                await conn.execute(downstream.update()
                    .where((downstream.c.annotation_id == annotation_id) &
                        (downstream.c.input_data_type == input_data_type) &
                        (downstream.c.analysis_type == "gsea")
                    )
                    .values(
                        slurm_job_id = jobID,
                        slurm_job_state = "PENDING",
                        result_path = userDir.absolute().__str__(),
                    )
                )
                await transaction.commit()
        else:
            async with conn.begin() as transaction:
                await conn.execute(downstream.insert().values(
                    user_id = int(data["user_id"]),
                    annotation_id = annotation_id,
                    input_data_type = input_data_type,
                    slurm_job_id = jobID,
                    slurm_job_state = "PENDING",
                    analysis_type = "gsea",
                    result_path = userDir.absolute().__str__(),
                ))
                await transaction.commit()

        return web.Response(text="OK")

# need thread parameters for frontend and R for calling gsva function
async def scRNA_GSVA(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    load_data_r_code = ''
    qc_r_code = ''
    combined_r_code = ''
    dc_r_code = ''
    anno_type_r_code = ''
    anno_subtype_r_code = ''
    gsva_r_code = ''

    right_species = ''

    # 1. 
    async with request.app['mysql_db'].acquire() as conn:
        sql = '''
            SELECT a.r_code, b.r_code
            FROM annotation_type a, upstream_dc b
            WHERE a.upstream_dc_id = b.id AND a.id = $anno_cluster_id;
        '''
        sql = sql.replace("$anno_cluster_id", str(data["anno_cluster_id"]))
        res = await conn.execute(sql)
        row = await res.fetchone()
        anno_type_r_code = row[0]
        dc_r_code = row[1]

        if str(data["anno_subcluster_id"]) != '-':
            res1 = await conn.execute(annotation_subtype.select()
                .where(annotation_subtype.c.id == int(data["anno_subcluster_id"]))
            )
            row1 = await res1.fetchone()
            anno_subtype_r_code = row1["dc_r_code"] + '\n' + row1["anno_r_code"] + '\n'

        if str(data["combined_id"]) != '-':
            # get combined_r_code
            if str(data["combined_type"]) == 'merge':
                res2 = await conn.execute(upstream_merge.select()
                    .where(upstream_merge.c.id == int(data["combined_id"]))
                )
                row2 = await res2.fetchone()
                combined_r_code = row2["r_code"]
            else:
                res2 = await conn.execute(upstream_integrate.select()
                    .where(upstream_integrate.c.id == int(data["combined_id"]))
                )
                row2 = await res2.fetchone()
                combined_r_code = row2["r_code"]

            # get qc_r_code load_data_r_code
            # merge/integrate data are not generated from generate_loom result
            for x in str(data["combined_data_ids"]).split(':'):
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(x)))
                row3 = await res3.fetchone()
                rbaseName = 'seurat_' + x
                right_species = row3["species"]

                if row3["data_type"] == "fastq":
                    res4 = await conn.execute(upstream_reads_mapping.select()
                        .where(upstream_reads_mapping.c.upload_data_id == row3["id"])
                    )
                    row4 = await res4.fetchone()
                    load_data_r_code += rbaseName+'.data <- Read10X("'+row4["result_path"]+'")\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res5 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.input_data_id == row4["id"])
                    )
                    row5 = await res5.fetchone()
                    qc_r_code += row5["r_code"]+"\n"

                elif row3["data_type"] == "cellranger":
                    load_data_r_code += rbaseName+'.data <- Read10X("'+row3["upload_path"]+'")\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res4 = await conn.execute(upstream_quality_control.select()
                            .where(upstream_quality_control.c.input_data_id == row3["id"])
                        )
                    row4 = await res4.fetchone()
                    qc_r_code += row4["r_code"] + "\n"

                else: # row3["data_type"] == "matrix"
                    load_data_r_code = rbaseName+'.data <- read.table(file="'+row3["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res4 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.input_data_id == row3["id"])
                    )
                    row4 = await res4.fetchone()
                    qc_r_code += row4["r_code"] + "\n"

        else: # str(data["combined_id"]) == '-':
            # get qc_r_code
            res2 = await conn.execute(upstream_quality_control.select()
                .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
            )
            row2 = await res2.fetchone()
            qc_r_code = row2["r_code"]
            # get load_data_r_code
            rbaseName = 'seurat_' + str(data["data_id"])
            if str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) == "-":
                resxx = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                rowxx = await resxx.fetchone()
                right_species = rowxx["species"]

                res3 = await conn.execute(upstream_reads_mapping.select().
                        where(upstream_reads_mapping.c.upload_data_id == int(data["reads_mapping_id"]))
                    )
                row3 = await res3.fetchone()
                load_data_r_code = rbaseName+'.data <- Read10X("'+row3["result_path"]+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                
            elif str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) != "-":
                resxx = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                rowxx = await resxx.fetchone()
                right_species = rowxx["species"]

                res3 = await conn.execute(upstream_generate_loom.select()
                        .where(upstream_generate_loom.c.id == int(data["generate_loom_id"]))
                    )
                row3 = await res3.fetchone()
                loom_file = row3['result_path'] + '/cellranger.loom'
                load_data_r_code = rbaseName+' <- read.loom.matrices(file = "'+loom_file+'")\n'+\
                    rbaseName+' <- as.Seurat('+rbaseName+')\n'+\
                    rbaseName+'[["RNA"]] <- '+rbaseName+'[["spliced"]]\n'+\
                    'DefaultAssay('+rbaseName+') <- "RNA"\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

            elif str(data["data_type"]) == 'cellranger':
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                row3 = await res3.fetchone()
                right_species = row3["species"]
                load_data_r_code = rbaseName+'.data <- Read10X("'+row3["upload_path"]+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

            elif str(data["data_type"]) == 'matrix':
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                row3 = await res3.fetchone()
                right_species = row3["species"]
                load_data_r_code = rbaseName+'.data <- read.table(file="'+row3["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
            else:
                pass
    

    # 3. generate corelation_r_code

    hsa_gene_set = {} # geneset gmt file name storage
    mmu_gene_set = {}
    # human geneset
    hsa_gene_set["C1"] = "c1.all.v2023.1.Hs.symbols.gmt"
    hsa_gene_set["C2"] = "c2.all.v2023.1.Hs.symbols.gmt"
    hsa_gene_set["C3"] = "c3.all.v2023.1.Hs.symbols.gmt"
    hsa_gene_set["C4"] = "c4.all.v2023.1.Hs.symbols.gmt"
    hsa_gene_set["C5"] = "c5.all.v2023.1.Hs.symbols.gmt"
    hsa_gene_set["C6"] = "c6.all.v2023.1.Hs.symbols.gmt"
    hsa_gene_set["C7"] = "c7.all.v2023.1.Hs.symbols.gmt"
    hsa_gene_set["C8"] = "c8.all.v2023.1.Hs.symbols.gmt"
    hsa_gene_set["H"] = "h.all.v2023.1.Hs.symbols.gmt"
    # mouse geneset
    mmu_gene_set["MH"] = "mh.all.v2023.1.Mm.symbols.gmt"
    mmu_gene_set["M1"] = "m1.all.v2023.1.Mm.symbols.gmt"
    mmu_gene_set["M2"] = "m2.all.v2023.1.Mm.symbols.gmt"
    mmu_gene_set["M3"] = "m3.all.v2023.1.Mm.symbols.gmt"
    mmu_gene_set["M5"] = "m5.all.v2023.1.Mm.symbols.gmt"
    mmu_gene_set["M8"] = "m8.all.v2023.1.Mm.symbols.gmt"

    if right_species == 'human' and data["gene_set"] in hsa_gene_set:
        gmt_file = str(config["geneSetGmtFilePath"]) + "/" + hsa_gene_set[data["gene_set"]]
        
    elif right_species == 'human' and data["gene_set"] not in hsa_gene_set:
        return web.Response(text="error: species: human, gene_set error")
    
    elif right_species == 'mouse' and data["gene_set"] in mmu_gene_set:
        gmt_file = str(config["geneSetGmtFilePath"]) + "/" + mmu_gene_set[data["gene_set"]]
        
    elif right_species == 'mouse' and data["gene_set"] not in mmu_gene_set:
        return web.Response(text="error: species: mouse, gene_set error")
    else:
        pass

    if str(data["anno_subcluster_id"]) == '-':
        if str(data["combined_id"]) != "-":
            gsva_r_code = 's.sets = gmt2list("'+gmt_file+'")\n'+\
            '''
expr <- as.matrix(project.combined@assays$RNA@counts)
es.matrix = gsva(expr, s.sets, kcdf="Poisson", parallel.sz = 40)
meta <- data.frame(annotation = project.combined@meta.data[,"check_clusters"])
rownames(meta) <- colnames(es.matrix)
pheatmap(es.matrix, show_rownames=1, show_colnames=0, annotation_col=meta,
         fontsize_row=5, filename='gsva.png', width=15, height=12)
            '''

        else:
            gsva_r_code = 'meta <- data.frame(annotation = '+rbaseName+'@meta.data[,"check_clusters"])\n'+\
            'expr <- as.matrix('+rbaseName+'@assays$RNA@counts)\n'+\
            's.sets = gmt2list("'+gmt_file+'")\n'+\
            '''
es.matrix = gsva(expr, s.sets, kcdf="Poisson")
rownames(meta) <- colnames(es.matrix)
pheatmap(es.matrix, show_rownames=1, show_colnames=0, annotation_col=meta,
         fontsize_row=5, filename='gsva.png', width=15, height=12)
            '''
    else:# str(data["anno_subcluster_id"]) != '-'
            gsva_r_code = 's.sets = gmt2list("'+gmt_file+'")\n'+\
            '''
expr <- as.matrix(subcluster@assays$RNA@counts)
meta <- data.frame(annotation = subcluster@meta.data[,"check_clusters"])
es.matrix = gsva(expr, s.sets, kcdf="Poisson")
rownames(meta) <- colnames(es.matrix)
pheatmap(es.matrix, show_rownames=1, show_colnames=0, annotation_col=meta,
         fontsize_row=5, filename='gsva.png', width=15, height=12)
            '''

    # 4. generate script file and submit slurm job
    script = r_script_GSVA.replace("#loadDataRcode", load_data_r_code)
    script = script.replace("#qcRcode", qc_r_code)
    if str(data["combined_id"]) != "-":
        script = script.replace("#combinedRcode", combined_r_code)
    script = script.replace("#dcRcode", dc_r_code)
    script = script.replace("#annotationTypeRcode", anno_type_r_code)
    if str(data["anno_subcluster_id"]) != '-':
        script = script.replace("#annotationSubtypeRcode", anno_subtype_r_code)
    script = script.replace("#GSVARcode", gsva_r_code)

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="scRNA_gsva",
        suffix=".R",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file:
        await file.write(script)

    baseDir = pathlib.Path(config["gsvaResultPath"])
    userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    while userDir.exists():
        userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    await asyncio.to_thread(userDir.mkdir, parents=True)
    
    script1 = downstream_bash.replace("partition", config["slurmPartition"])
    script1 = script1.replace("jobLogPath", config["slurmJobLogPath"])
    script1 = script1.replace("downstreamCode", "/usr/bin/Rscript " + file.name)
    script1 = script1.replace("userDir", userDir.absolute().__str__())
    script1 = script1.replace("$localcores", '1')

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="scRNA_gsva",
        suffix=".sh",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file1:
        await file1.write(script1)

    """
    cmd = "sbatch "+file1.name
    proc = await asyncio.create_subprocess_shell(
        cmd, 
        stdout=asyncio.subprocess.PIPE, 
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    x = stdout.decode().split()
    jobID = int(x[-1])
    """

    result = await asyncio.to_thread(subprocess.check_output, ['/usr/local/bin/sbatch', str(file1.name)])
    output = result.decode('utf-8')
    x = output.split()
    jobID = int(x[-1])
    
    # 5. insert value to downstream table
    async with lock:
        slurm_jobs[jobID] = 'gsva'

    if str(data["anno_subcluster_id"]) != "-":
        input_data_type = "annotation_subtype"
        annotation_id = int(data["anno_subcluster_id"])
    else:
        input_data_type = "annotation_type"
        annotation_id = int(data["anno_cluster_id"])

    async with request.app['mysql_db'].acquire() as conn:
        resx = await conn.execute(downstream.select()
                .where((downstream.c.annotation_id == annotation_id) &
                       (downstream.c.input_data_type == input_data_type) &
                       (downstream.c.analysis_type == "gsva")
                )
            )
        rowx = await resx.fetchone()
        if rowx:
            # 删除原本的分群结果目录
            rmtree(str(rowx["result_path"]))
            # 更新数据库中的结果
            async with conn.begin() as transaction:
                await conn.execute(downstream.update()
                    .where((downstream.c.annotation_id == annotation_id) &
                        (downstream.c.input_data_type == input_data_type) &
                        (downstream.c.analysis_type == "gsva")
                    )
                    .values(
                        slurm_job_id = jobID,
                        slurm_job_state = "PENDING",
                        result_path = userDir.absolute().__str__(),
                    )
                )
                await transaction.commit()
        else:
            async with conn.begin() as transaction:
                await conn.execute(downstream.insert().values(
                    user_id = int(data["user_id"]),
                    annotation_id = annotation_id,
                    input_data_type = input_data_type,
                    slurm_job_id = jobID,
                    slurm_job_state = "PENDING",
                    analysis_type = "gsva",
                    result_path = userDir.absolute().__str__(),
                ))
                await transaction.commit()

        return web.Response(text="OK")

# RNA velocity can only apply for generate loom file data
async def scRNA_velocity(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    load_data_r_code = ''
    qc_r_code = ''
    dc_r_code = ''
    anno_type_r_code = ''
    rbaseName = 'seurat_' + str(data["data_id"])

    async with request.app['mysql_db'].acquire() as conn:
        sql = '''
            SELECT a.r_code, b.r_code, c.r_code
            FROM annotation_type a, upstream_dc b, upstream_quality_control c
            WHERE a.upstream_dc_id = b.id AND a.id = $anno_cluster_id AND c.id = b.input_data_id;
        '''
        sql = sql.replace("$anno_cluster_id", str(data["anno_cluster_id"]))
        res = await conn.execute(sql)
        row = await res.fetchone()
        anno_type_r_code = row[0]
        dc_r_code = row[1]
        qc_r_code = row[2]

        res1 = await conn.execute(upstream_generate_loom.select()
                    .where(upstream_generate_loom.c.id == int(data["generate_loom_id"]))
                )
        row1 = await res1.fetchone()
        loom_file = row1['result_path'] + '/cellranger.loom'
        load_data_r_code = rbaseName+' <- read.loom.matrices(file = "'+loom_file+'")\n'+\
                rbaseName+' <- as.Seurat('+rbaseName+')\n'+\
                rbaseName+'[["RNA"]] <- '+rbaseName+'[["spliced"]]\n'+\
                'DefaultAssay('+rbaseName+') <- "RNA"\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'


    # 3. generate RNA velocity code

    velocity_r_code = 'DefaultAssay('+rbaseName+') <- "RNA"\n'+\
        'SaveH5Seurat('+rbaseName+', filename = "'+rbaseName+'.h5Seurat")\n'+\
        'Convert("'+rbaseName+'.h5Seurat", dest = "h5ad")\n'

    # 4. generate script file and submit slurm job 
    script = r_script_velocity.replace("#loadDataRcode", load_data_r_code)
    script = script.replace("#qcRcode", qc_r_code)
    script = script.replace("#dcRcode", dc_r_code)
    script = script.replace("#annotationTypeRcode", anno_type_r_code)
    script = script.replace("#velocityRcode", velocity_r_code)

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="velocity",
        suffix=".R",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file:
        await file.write(script)

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="velocity",
        suffix=".py",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file1:
        await file1.write(py_script_velocity.replace("$H5adFile", rbaseName+"..h5ad"))

    baseDir = pathlib.Path(config["velocityResultPath"])
    userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    while userDir.exists():
        userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    await asyncio.to_thread(userDir.mkdir, parents=True)
    
    script1 = downstream_bash.replace("partition", config["slurmPartition"])
    script1 = script1.replace("jobLogPath", config["slurmJobLogPath"])
    script1 = script1.replace("downstreamCode", "/usr/bin/Rscript " + file.name)
    script1 += 'export PATH=/home/deng/miniconda3/envs/scvelo/bin:/home/deng/go/bin:/home/deng/miniconda3/condabin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin\n'
    script1 += 'python ' + file1.name
    script1 = script1.replace("userDir", userDir.absolute().__str__())
    script1 = script1.replace("$localcores", '1')

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="velocity",
        suffix=".sh",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file2:
        await file2.write(script1)

    """
    cmd = "sbatch "+file2.name
    proc = await asyncio.create_subprocess_shell(
        cmd, 
        stdout=asyncio.subprocess.PIPE, 
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    x = stdout.decode().split()
    jobID = int(x[-1])
    """

    result = await asyncio.to_thread(subprocess.check_output, ['/usr/local/bin/sbatch', str(file2.name)]) 
    output = result.decode('utf-8')
    x = output.split()
    jobID = int(x[-1])
    
    # 5. insert value to downstream table
    async with lock:
        slurm_jobs[jobID] = 'velocity'

    input_data_type = "annotation_type"
    annotation_id = int(data["anno_cluster_id"])

    async with request.app['mysql_db'].acquire() as conn:
        resx = await conn.execute(downstream.select()
                .where((downstream.c.annotation_id == annotation_id) &
                       (downstream.c.input_data_type == input_data_type) &
                       (downstream.c.analysis_type == "velocity")
                )
            )
        rowx = await resx.fetchone()
        if rowx:
            # 删除原本的分群结果目录
            rmtree(str(rowx["result_path"]))
            # 更新数据库中的结果
            async with conn.begin() as transaction:
                await conn.execute(downstream.update()
                    .where((downstream.c.annotation_id == annotation_id) &
                        (downstream.c.input_data_type == input_data_type) &
                        (downstream.c.analysis_type == "velocity")
                    )
                    .values(
                        slurm_job_id = jobID,
                        slurm_job_state = "PENDING",
                        result_path = userDir.absolute().__str__(),
                    )
                )
                await transaction.commit()
        else:
            async with conn.begin() as transaction:
                await conn.execute(downstream.insert().values(
                    user_id = int(data["user_id"]),
                    annotation_id = annotation_id,
                    input_data_type = input_data_type,
                    slurm_job_id = jobID,
                    slurm_job_state = "PENDING",
                    analysis_type = "velocity",
                    result_path = userDir.absolute().__str__(),
                ))
                await transaction.commit()

        return web.Response(text="OK")

# trajectory analysis used to apply for subcluster
# all culster trajectory has no means
async def scRNA_trajectory(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    load_data_r_code = ''
    qc_r_code = ''
    combined_r_code = ''
    dc_r_code = ''
    anno_type_r_code = ''
    anno_subtype_r_code = ''

    # 1. 
    async with request.app['mysql_db'].acquire() as conn:
        sql = '''
            SELECT a.r_code, b.r_code
            FROM annotation_type a, upstream_dc b
            WHERE a.upstream_dc_id = b.id AND a.id = $anno_cluster_id;
        '''
        sql = sql.replace("$anno_cluster_id", str(data["anno_cluster_id"]))
        res = await conn.execute(sql)
        row = await res.fetchone()
        anno_type_r_code = row[0]
        dc_r_code = row[1]

        if str(data["anno_subcluster_id"]) != '-':
            res1 = await conn.execute(annotation_subtype.select()
                .where(annotation_subtype.c.id == int(data["anno_subcluster_id"]))
            )
            row1 = await res1.fetchone()
            anno_subtype_r_code = row1["dc_r_code"] + '\n' + row1["anno_r_code"] + '\n'

        if str(data["combined_id"]) != '-':
            # get combined_r_code
            if str(data["combined_type"]) == 'merge':
                res2 = await conn.execute(upstream_merge.select()
                    .where(upstream_merge.c.id == int(data["combined_id"]))
                )
                row2 = await res2.fetchone()
                combined_r_code = row2["r_code"]
            else:
                res2 = await conn.execute(upstream_integrate.select()
                    .where(upstream_integrate.c.id == int(data["combined_id"]))
                )
                row2 = await res2.fetchone()
                combined_r_code = row2["r_code"]

            # get qc_r_code load_data_r_code
            # merge/integrate data are not generated from generate_loom result
            for x in str(data["combined_data_ids"]).split(':'):
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(x)))
                row3 = await res3.fetchone()
                rbaseName = 'seurat_' + x

                if row3["data_type"] == "fastq":
                    res4 = await conn.execute(upstream_reads_mapping.select()
                        .where(upstream_reads_mapping.c.upload_data_id == row3["id"])
                    )
                    row4 = await res4.fetchone()
                    load_data_r_code += rbaseName+'.data <- Read10X("'+row4["result_path"]+'")\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res5 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.input_data_id == row4["id"])
                    )
                    row5 = await res5.fetchone()
                    qc_r_code += row5["r_code"]+"\n"

                elif row3["data_type"] == "cellranger":
                    load_data_r_code += rbaseName+'.data <- Read10X("'+row3["upload_path"]+'")\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res4 = await conn.execute(upstream_quality_control.select()
                            .where(upstream_quality_control.c.input_data_id == row3["id"])
                        )
                    row4 = await res4.fetchone()
                    qc_r_code += row4["r_code"] + "\n"

                else: # row3["data_type"] == "matrix"
                    load_data_r_code = rbaseName+'.data <- read.table(file="'+row3["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res4 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.input_data_id == row3["id"])
                    )
                    row4 = await res4.fetchone()
                    qc_r_code += row4["r_code"] + "\n"

        else: # str(data["combined_id"]) == '-':
            # get qc_r_code
            res2 = await conn.execute(upstream_quality_control.select()
                .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
            )
            row2 = await res2.fetchone()
            qc_r_code = row2["r_code"]
            # get load_data_r_code
            rbaseName = 'seurat_' + str(data["data_id"])
            if str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) == "-":
                res3 = await conn.execute(upstream_reads_mapping.select().
                        where(upstream_reads_mapping.c.upload_data_id == int(data["reads_mapping_id"]))
                    )
                row3 = await res3.fetchone()
                load_data_r_code = rbaseName+'.data <- Read10X("'+row3["result_path"]+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                
            elif str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) != "-":
                res3 = await conn.execute(upstream_generate_loom.select()
                        .where(upstream_generate_loom.c.id == int(data["generate_loom_id"]))
                    )
                row3 = await res3.fetchone()
                loom_file = row3['result_path'] + '/cellranger.loom'
                load_data_r_code = rbaseName+' <- read.loom.matrices(file = "'+loom_file+'")\n'+\
                    rbaseName+' <- as.Seurat('+rbaseName+')\n'+\
                    rbaseName+'[["RNA"]] <- '+rbaseName+'[["spliced"]]\n'+\
                    'DefaultAssay('+rbaseName+') <- "RNA"\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

            elif str(data["data_type"]) == 'cellranger':
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                row3 = await res3.fetchone()
                load_data_r_code = rbaseName+'.data <- Read10X("'+row3["upload_path"]+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

            elif str(data["data_type"]) == 'matrix':
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                row3 = await res3.fetchone()
                load_data_r_code = rbaseName+'.data <- read.table(file="'+row3["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
            else:
                pass

    # 3. generate trajectory_r_code
    trajectory_r_code = ''
    if str(data["anno_subcluster_id"]) == '-':
        if str(data["combined_id"]) != '-':
            if str(data["analysis_type"]) == "monocle3":
                trajectory_r_code = 'Idents(project.combined) <- "check_clusters"\n'+\
                'cell.use <- WhichCells(project.combined, idents = "'+str(data["root_cells"])+'")\n'+\
                r'''
cds <- as.cell_data_set(project.combined)
cds <- cluster_cells(cds)
cds <- learn_graph(cds)
cds <- order_cells(cds, root_cells = cell.use)
p_seurat <- DimPlot(project.combined)
seurat_theme <- p_seurat$theme
p <- plot_cells(cds, color_cells_by = "pseudotime", label_cell_groups = FALSE, label_leaves = FALSE, label_branch_points = FALSE) + seurat_theme
ggsave(filename = "trajectory.png", plot=p,width = 12, height = 8,dpi=600,limitsize = F)

            '''
            else: # str(data["analysis_type"]) == "monocle2":
                trajectory_r_code = 'DefaultAssay(project.combined) <- "RNA"\n'+\
                'project.combined <- FindVariableFeatures(project.combined)\n'+\
                'cds_ordering_genes <- VariableFeatures(project.combined, assay = "RNA")[ 1:'+str(data["nvfeatures"])+']\n'+\
                r'''
data <- as(as.matrix(project.combined@assays$RNA@counts), 'sparseMatrix')
pd <- new('AnnotatedDataFrame', data = project.combined@meta.data)
fData <- data.frame(gene_short_name = row.names(data), row.names = row.names(data))
fd <- new('AnnotatedDataFrame', data = fData)
cds <- newCellDataSet(cellData=data,
                      phenoData = pd,
                      featureData = fd)
cds <- estimateSizeFactors(cds)
cds <- estimateDispersions(cds)

Idents(project.combined) <- "check_clusters"
cds <- setOrderingFilter( cds, ordering_genes=cds_ordering_genes )
cds <- reduceDimension( cds, max_components = 2, reduction_method ="DDRTree" )
cds <- orderCells(cds)
cds <- orderCells(cds, root_state = 1)

p_seurat <- DimPlot(project.combined)
seurat_theme <- p_seurat$theme

p <- plot_cell_trajectory(cds, color_by = "check_clusters") + seurat_theme
p1 <- plot_cell_trajectory(cds, color_by = "Pseudotime") + seurat_theme
ggsave(filename = "trajectory.png", plot=p, width = 12, height = 8, dpi=600, limitsize = F)
ggsave(filename = "pseudotime.png", plot=p1, width = 12, height = 8, dpi=600, limitsize = F)
'''
                

        else: # single sample data : str(data["combined_id"]) == '-'
            if str(data["analysis_type"]) == "monocle3":
                trajectory_r_code = 'Idents('+rbaseName+') <- "check_clusters"\n'+\
                'cell.use <- WhichCells('+rbaseName+', idents = "'+str(data["root_cells"])+'")\n'+\
                'cds <- as.cell_data_set('+rbaseName+')\n'+\
                'p_seurat <- DimPlot('+rbaseName+')\n'+\
                r'''
cds <- cluster_cells(cds)
cds <- learn_graph(cds)
cds <- order_cells(cds, root_cells = cell.use)
seurat_theme <- p_seurat$theme
p <- plot_cells(cds, color_cells_by = "pseudotime", label_cell_groups = FALSE, label_leaves = FALSE, label_branch_points = FALSE) + seurat_theme
ggsave(filename = "trajectory.png", plot=p,width = 12, height = 8,dpi=600,limitsize = F)            
            '''
            else: # str(data["analysis_type"]) == "monocle2"
                trajectory_r_code = 'Idents('+rbaseName+') <- "check_clusters"\n'+\
                'DefaultAssay('+rbaseName+') <- "RNA"\n'+\
                rbaseName + ' <- FindVariableFeatures(project.combined)\n'+\
                'p_seurat <- DimPlot('+rbaseName+')\n'+\
                'cds_ordering_genes <- VariableFeatures('+rbaseName+', assay = "RNA")[ 1:'+str(data["nvfeatures"])+']\n'+\
                'data <- as(as.matrix('+rbaseName+'@assays$RNA@counts), "sparseMatrix")\n'+\
                'pd <- new("AnnotatedDataFrame", data = '+rbaseName+'@meta.data)\n'+\
                r'''
fData <- data.frame(gene_short_name = row.names(data), row.names = row.names(data))
fd <- new('AnnotatedDataFrame', data = fData)
cds <- newCellDataSet(cellData=data,
                      phenoData = pd,
                      featureData = fd)
cds <- estimateSizeFactors(cds)
cds <- estimateDispersions(cds)

cds <- setOrderingFilter( cds, ordering_genes=cds_ordering_genes )
cds <- reduceDimension( cds, max_components = 2, reduction_method ="DDRTree" )
cds <- orderCells(cds)
cds <- orderCells(cds, root_state = 1)

seurat_theme <- p_seurat$theme

p <- plot_cell_trajectory(cds, color_by = "check_clusters") + seurat_theme
p1 <- plot_cell_trajectory(cds, color_by = "Pseudotime") + seurat_theme
ggsave(filename = "trajectory.png", plot=p, width = 12, height = 8, dpi=600, limitsize = F)
ggsave(filename = "pseudotime.png", plot=p1, width = 12, height = 8, dpi=600, limitsize = F)
'''                

    else:# str(data["anno_subcluster_id"]) != '-'
        if str(data["analysis_type"]) == "monocle3":
            trajectory_r_code = 'cell.use <- WhichCells(subcluster, idents = "'+str(data["root_cells"])+'")\n'+\
            r'''
cds <- as.cell_data_set(subcluster)
cds <- cluster_cells(cds)
cds <- learn_graph(cds)
cds <- order_cells(cds, root_cells = cell.use)
p_seurat <- DimPlot(subcluster)
seurat_theme <- p_seurat$theme
p <- plot_cells(cds, color_cells_by = "pseudotime", label_cell_groups = FALSE, label_leaves = FALSE, label_branch_points = FALSE) + seurat_theme
ggsave(filename = "trajectory.png", plot=p,width = 12, height = 8,dpi=600,limitsize = F)
            '''
        else: #str(data["analysis_type"]) == "monocle2"
            trajectory_r_code = 'DefaultAssay(subcluster) <- "RNA"\n'+\
            'subcluster <- FindVariableFeatures(subcluster)\n'+\
            'cds_ordering_genes <- VariableFeatures(subcluster, assay = "RNA")[ 1:'+str(data["nvfeatures"])+']\n'+\
            r"""
data <- as(as.matrix(subcluster@assays$RNA@counts), 'sparseMatrix')
pd <- new('AnnotatedDataFrame', data = subcluster@meta.data)
fData <- data.frame(gene_short_name = row.names(data), row.names = row.names(data))
fd <- new('AnnotatedDataFrame', data = fData)
cds <- newCellDataSet(cellData=data,
                      phenoData = pd,
                      featureData = fd)
cds <- estimateSizeFactors(cds)
cds <- estimateDispersions(cds)

Idents(subcluster) <- "check_clusters"

cds <- setOrderingFilter( cds, ordering_genes=cds_ordering_genes )
cds <- reduceDimension( cds, max_components = 2, reduction_method ="DDRTree" )
cds <- orderCells(cds)
cds <- orderCells(cds, root_state = 1)

p_seurat <- DimPlot(subcluster)
seurat_theme <- p_seurat$theme

p <- plot_cell_trajectory(cds, color_by = "check_clusters") + seurat_theme
p1 <- plot_cell_trajectory(cds, color_by = "Pseudotime") + seurat_theme
ggsave(filename = "trajectory.png", plot=p, width = 12, height = 8, dpi=600, limitsize = F)
ggsave(filename = "pseudotime.png", plot=p1, width = 12, height = 8, dpi=600, limitsize = F)
"""


    # 4. generate script file and submit slurm job
    if str(data["analysis_type"]) == "monocle3":
        script = r_script_trajectory_monocle3.replace("#loadDataRcode", load_data_r_code)
    else:
        script = r_script_trajectory_monocle2.replace("#loadDataRcode", load_data_r_code)

    script = script.replace("#qcRcode", qc_r_code)
    if str(data["combined_id"]) != '-':
        script = script.replace("#combinedRcode", combined_r_code)
    script = script.replace("#dcRcode", dc_r_code)
    script = script.replace("#annotationTypeRcode", anno_type_r_code)
    if str(data["anno_subcluster_id"]) != '-':
        script = script.replace("#annotationSubtypeRcode", anno_subtype_r_code)
    script = script.replace("#trajectoryRcode", trajectory_r_code)

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="trajectory",
        suffix=".R",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file:
        await file.write(script)

    baseDir = pathlib.Path(config["trajectoryResultPath"])
    userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    while userDir.exists():
        userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    await asyncio.to_thread(userDir.mkdir, parents=True)
    
    script1 = downstream_bash.replace("partition", config["slurmPartition"])
    script1 = script1.replace("jobLogPath", config["slurmJobLogPath"])
    script1 = script1.replace("downstreamCode", "/usr/bin/Rscript " + file.name)
    script1 = script1.replace("userDir", userDir.absolute().__str__())
    script1 = script1.replace("$localcores", '1')

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="trajectory",
        suffix=".sh",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file1:
        await file1.write(script1)

    """
    cmd = "sbatch "+file1.name
    proc = await asyncio.create_subprocess_shell(
        cmd, 
        stdout=asyncio.subprocess.PIPE, 
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    x = stdout.decode().split()
    jobID = int(x[-1])
    """

    result = await asyncio.to_thread(subprocess.check_output, ['/usr/local/bin/sbatch', str(file1.name)]) 
    output = result.decode('utf-8')
    x = output.split()
    jobID = int(x[-1])
    
    # 5. insert value to downstream table
    async with lock:
        slurm_jobs[jobID] = 'trajectory'

    if str(data["anno_subcluster_id"]) != "-":
        input_data_type = "annotation_subtype"
        annotation_id = int(data["anno_subcluster_id"])
    else:
        input_data_type = "annotation_type"
        annotation_id = int(data["anno_cluster_id"])

    async with request.app['mysql_db'].acquire() as conn:
        resx = await conn.execute(downstream.select()
                .where((downstream.c.annotation_id == annotation_id) &
                       (downstream.c.input_data_type == input_data_type) &
                       (downstream.c.analysis_type == "trajectory")
                )
            )
        rowx = await resx.fetchone()
        if rowx:
            # 删除原本的分群结果目录
            rmtree(str(rowx["result_path"]))
            # 更新数据库中的结果
            async with conn.begin() as transaction:
                await conn.execute(downstream.update()
                    .where((downstream.c.annotation_id == annotation_id) &
                        (downstream.c.input_data_type == input_data_type) &
                        (downstream.c.analysis_type == "trajectory")
                    )
                    .values(
                        slurm_job_id = jobID,
                        slurm_job_state = "PENDING",
                        result_path = userDir.absolute().__str__(),
                    )
                )
                await transaction.commit()
        else:
            async with conn.begin() as transaction:
                await conn.execute(downstream.insert().values(
                    user_id = int(data["user_id"]),
                    annotation_id = annotation_id,
                    input_data_type = input_data_type,
                    slurm_job_id = jobID,
                    slurm_job_state = "PENDING",
                    analysis_type = "trajectory",
                    result_path = userDir.absolute().__str__(),
                ))
                await transaction.commit()


        return web.Response(text="OK")


async def scRNA_cell_communication(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    load_data_r_code = ''
    qc_r_code = ''
    combined_r_code = ''
    dc_r_code = ''
    anno_type_r_code = ''
    anno_subtype_r_code = ''
    cellphoneDB_r_code = ''

    right_species = ''

    # 1. 
    async with request.app['mysql_db'].acquire() as conn:
        sql = '''
            SELECT a.r_code, b.r_code
            FROM annotation_type a, upstream_dc b
            WHERE a.upstream_dc_id = b.id AND a.id = $anno_cluster_id;
        '''
        sql = sql.replace("$anno_cluster_id", str(data["anno_cluster_id"]))
        res = await conn.execute(sql)
        row = await res.fetchone()
        anno_type_r_code = row[0]
        dc_r_code = row[1]

        if str(data["anno_subcluster_id"]) != '-':
            res1 = await conn.execute(annotation_subtype.select()
                .where(annotation_subtype.c.id == int(data["anno_subcluster_id"]))
            )
            row1 = await res1.fetchone()
            anno_subtype_r_code = row1["dc_r_code"] + '\n' + row1["anno_r_code"] + '\n'

        if str(data["combined_id"]) != '-':
            # get combined_r_code
            if str(data["combined_type"]) == 'merge':
                res2 = await conn.execute(upstream_merge.select()
                    .where(upstream_merge.c.id == int(data["combined_id"]))
                )
                row2 = await res2.fetchone()
                combined_r_code = row2["r_code"]
            else:
                res2 = await conn.execute(upstream_integrate.select()
                    .where(upstream_integrate.c.id == int(data["combined_id"]))
                )
                row2 = await res2.fetchone()
                combined_r_code = row2["r_code"]

            # get qc_r_code load_data_r_code
            # merge/integrate data are not generated from generate_loom result
            for x in str(data["combined_data_ids"]).split(':'):
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(x)))
                row3 = await res3.fetchone()
                rbaseName = 'seurat_' + x
                right_species = row3["species"]

                if row3["data_type"] == "fastq":
                    res4 = await conn.execute(upstream_reads_mapping.select()
                        .where(upstream_reads_mapping.c.upload_data_id == row3["id"])
                    )
                    row4 = await res4.fetchone()
                    load_data_r_code += rbaseName+'.data <- Read10X("'+row4["result_path"]+'")\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res5 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.input_data_id == row4["id"])
                    )
                    row5 = await res5.fetchone()
                    qc_r_code += row5["r_code"]+"\n"

                elif row3["data_type"] == "cellranger":
                    load_data_r_code += rbaseName+'.data <- Read10X("'+row3["upload_path"]+'")\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res4 = await conn.execute(upstream_quality_control.select()
                            .where(upstream_quality_control.c.input_data_id == row3["id"])
                        )
                    row4 = await res4.fetchone()
                    qc_r_code += row4["r_code"] + "\n"

                else: # row3["data_type"] == "matrix"
                    load_data_r_code = rbaseName+'.data <- read.table(file="'+row3["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res4 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.input_data_id == row3["id"])
                    )
                    row4 = await res4.fetchone()
                    qc_r_code += row4["r_code"] + "\n"

        else: # str(data["combined_id"]) == '-':
            # get qc_r_code
            res2 = await conn.execute(upstream_quality_control.select()
                .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
            )
            row2 = await res2.fetchone()
            qc_r_code = row2["r_code"]
            # get load_data_r_code
            rbaseName = 'seurat_' + str(data["data_id"])
            if str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) == "-":
                resxx = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                rowxx = await resxx.fetchone()
                right_species = rowxx["species"]

                res3 = await conn.execute(upstream_reads_mapping.select().
                        where(upstream_reads_mapping.c.upload_data_id == int(data["reads_mapping_id"]))
                    )
                row3 = await res3.fetchone()
                load_data_r_code = rbaseName+'.data <- Read10X("'+row3["result_path"]+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                
            elif str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) != "-":
                resxx = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                rowxx = await resxx.fetchone()
                right_species = rowxx["species"]

                res3 = await conn.execute(upstream_generate_loom.select()
                        .where(upstream_generate_loom.c.id == int(data["generate_loom_id"]))
                    )
                row3 = await res3.fetchone()
                loom_file = row3['result_path'] + '/cellranger.loom'
                load_data_r_code = rbaseName+' <- read.loom.matrices(file = "'+loom_file+'")\n'+\
                    rbaseName+' <- as.Seurat('+rbaseName+')\n'+\
                    rbaseName+'[["RNA"]] <- '+rbaseName+'[["spliced"]]\n'+\
                    'DefaultAssay('+rbaseName+') <- "RNA"\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

            elif str(data["data_type"]) == 'cellranger':
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                row3 = await res3.fetchone()
                right_species = row3["species"]
                load_data_r_code = rbaseName+'.data <- Read10X("'+row3["upload_path"]+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

            elif str(data["data_type"]) == 'matrix':
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                row3 = await res3.fetchone()
                right_species = row3["species"]
                load_data_r_code = rbaseName+'.data <- read.table(file="'+row3["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
            else:
                pass
  
    
    # convert mouse gene to human gene symbols
    mouse_gene_code = 'Gene <- convert_mouse_to_human_symbols(Gene)\n'

    if str(data["combined_id"]) != '-':
        rbaseName = 'project.combined'
    
    if str(data["anno_subcluster_id"]) == '-':
        cellphoneDB_r_code = 'counts <- as.matrix('+rbaseName+'@assays$RNA@data)\n'+\
        'Gene <- rownames(counts)\n'
        if right_species == 'mouse':
            cellphoneDB_r_code += mouse_gene_code
        cellphoneDB_r_code += 'counts <- data.frame(Gene, counts)\n'+\
        r'counts <- counts %>% filter(Gene != "NA")'+\
        '\nmate <- data.frame(Cell=rownames('+rbaseName+'@meta.data), cell_type='+rbaseName+'@meta.data$check_clusters)'+\
        r'''
colnames(counts) <- gsub('\\.', '-', colnames(counts))
write.table(counts, file = 'c_counts.txt', row.names=F, sep='\t')
write.table(mate, file = 'c_mate.txt', row.names=F, sep='\t')
        '''
    else:# str(data["anno_subcluster_id"]) != '-'
        cellphoneDB_r_code = 'counts <- as.matrix(subcluster@assays$RNA@data)\n'+\
        'Gene <- rownames(counts)\n'
        if right_species == 'mouse':
            cellphoneDB_r_code += mouse_gene_code
        cellphoneDB_r_code += r'''
counts <- data.frame(Gene, counts)
counts <- counts %>% filter(Gene != "NA")
colnames(counts) <- gsub('\\.', '-', colnames(counts))
mate <- data.frame(Cell=rownames(subcluster@meta.data), cell_type=subcluster@meta.data$check_clusters)
write.table(counts, file = 'c_counts.txt', row.names=F, sep='\t', quote = F)
write.table(mate, file = 'c_mate.txt', row.names=F, sep='\t', quote = F)
        '''

    # using ktplots R package to plot result
    celltypes = str(data["celltypes"]).split(",")
    celltype_one = celltypes[0]

    celltype_two = ""
    for i, v in enumerate(celltypes):
        if i == 0 :
            continue
        if i == len(celltypes) - 1:
            celltype_two += v
        else:
            celltype_two += (v+"|")
    if str(data["anno_subcluster_id"]) != '-':
        rbaseName = "subcluster"
    visualize_r_code = r_script_visualizing_cellphoneDB.replace("$rbasename", rbaseName)
    visualize_r_code = visualize_r_code.replace("$celltype1", celltype_one)
    visualize_r_code = visualize_r_code.replace("$celltype2", celltype_two)

    # 4. generate script file and submit slurm job 
    script = r_script_cellphoneDB.replace("#loadDataRcode", load_data_r_code)
    script = script.replace("data_species", right_species)
    script = script.replace("#qcRcode", qc_r_code)
    if str(data["combined_id"])!="-":
        script = script.replace("#combinedRcode", combined_r_code)
    script = script.replace("#dcRcode", dc_r_code)
    script = script.replace("#annotationTypeRcode", anno_type_r_code)
    if str(data["anno_subcluster_id"]) != '-':
        script = script.replace("#annotationSubtypeRcode", anno_subtype_r_code)
    
    # visualize R script need load_data_r_code to anno_subtype_r_code
    visualize_r_script = script.replace("#cellphoneRcode", visualize_r_code)

    script = script.replace("#cellphoneRcode", cellphoneDB_r_code)

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="cellphone_visualize",
        suffix=".R",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file_v:
        await file_v.write(visualize_r_script)

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="cellphone",
        suffix=".R",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file:
        await file.write(script)

    baseDir = pathlib.Path(config["communicationResultPath"])
    userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    while userDir.exists():
        userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    await asyncio.to_thread(userDir.mkdir, parents=True)
    
    script1 = downstream_bash.replace("partition", config["slurmPartition"])
    script1 = script1.replace("jobLogPath", config["slurmJobLogPath"])
    script1 = script1.replace("downstreamCode", "/usr/bin/Rscript "+file.name)
    script1 += "export PATH=/home/deng/miniconda3/envs/cpdb/bin:$PATH\n"
    script1 += r'''
cellphonedb method statistical_analysis --counts-data gene_name --output-path ./ --iterations 1000 --threads 1 c_mate.txt c_counts.txt
cellphonedb plot heatmap_plot --pvalues-path pvalues.txt --output-path ./ --count-name cellphoneDB_heatmap_count.png c_mate.txt
sed 's#True#TRUE#g' deconvoluted.txt | sed 's#"##g' > decon2.txt
# visualize R script
    '''
    script1 += "/usr/bin/Rscript "+file_v.name + "\n"
    script1 = script1.replace("userDir", userDir.absolute().__str__())
    script1 = script1.replace("$localcores", '1')

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="cellphone",
        suffix=".sh",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file1:
        await file1.write(script1)

    """
    cmd = "sbatch "+file1.name
    proc = await asyncio.create_subprocess_shell(
        cmd, 
        stdout=asyncio.subprocess.PIPE, 
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    x = stdout.decode().split()
    jobID = int(x[-1])
    """

    result = await asyncio.to_thread(subprocess.check_output, ['/usr/local/bin/sbatch', str(file1.name)])
    output = result.decode('utf-8')
    x = output.split()
    jobID = int(x[-1])
    
    # 5. insert value to downstream table
    async with lock:
        slurm_jobs[jobID] = 'cellphonedb'

    if str(data["anno_subcluster_id"]) != "-":
        input_data_type = "annotation_subtype"
        annotation_id = int(data["anno_subcluster_id"])
    else:
        input_data_type = "annotation_type"
        annotation_id = int(data["anno_cluster_id"])

    async with request.app['mysql_db'].acquire() as conn:
        resx = await conn.execute(downstream.select()
                .where((downstream.c.annotation_id == annotation_id) &
                       (downstream.c.input_data_type == input_data_type) &
                       (downstream.c.analysis_type == "cellphonedb")
                )
            )
        rowx = await resx.fetchone()
        if rowx:
            # 删除原本的分群结果目录
            rmtree(str(rowx["result_path"]))
            # 更新数据库中的结果
            async with conn.begin() as transaction:
                await conn.execute(downstream.update()
                    .where((downstream.c.annotation_id == annotation_id) &
                        (downstream.c.input_data_type == input_data_type) &
                        (downstream.c.analysis_type == "cellphonedb")
                    )
                    .values(
                        slurm_job_id = jobID,
                        slurm_job_state = "PENDING",
                        result_path = userDir.absolute().__str__(),
                    )
                )
                await transaction.commit()
        else:
            async with conn.begin() as transaction:
                await conn.execute(downstream.insert().values(
                    user_id = int(data["user_id"]),
                    annotation_id = annotation_id,
                    input_data_type = input_data_type,
                    slurm_job_id = jobID,
                    slurm_job_state = "PENDING",
                    analysis_type = "cellphonedb",
                    result_path = userDir.absolute().__str__(),
                ))
                await transaction.commit()


        return web.Response(text="OK")

# 1. pyscenic error: AttributeError: module 'numpy' has no attribute 'object'.
# vim /home/deng/miniconda3/envs/scenic/lib/python3.10/site-packages/pyscenic/transform.py
# replace np.object with object
# 2. AttributeError: module 'numpy' has no attribute 'float'.
# vim /home/deng/miniconda3/envs/scenic/lib/python3.10/site-packages/pyscenic/diptest.py
# line 64 : replace np.float to float
# 
#
# the following code may solve above errors, from https://github.com/aertslab/pySCENIC/issues/475#issuecomment-1579052932
'''
conda create -y -n pyscenic_env python=3.10
conda activate pyscenic_env
pip install pyscenic   
pip install numpy==1.23.5 
pip install pandas==1.5.3 
pip install numba==0.56.4 
'''

# AttributeError: 'numpy.ndarray' object has no attribute 'split': error with loompy
# https://github.com/aertslab/pySCENIC/issues/112#issuecomment-1463731823
# vim /home/deng/miniconda3/envs/scenic/lib/python3.10/site-packages/loompy/utils.py
# line 27: vf = int("".join(get_loom_spec_version(f).split("."))) 
# to vf = int("".join(get_loom_spec_version(f)[0].split(".")))
async def scRNA_gene_regulatory_network(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    load_data_r_code = ''
    qc_r_code = ''
    combined_r_code = ''
    dc_r_code = ''
    anno_type_r_code = ''
    anno_subtype_r_code = ''

    right_species = ''

    # 1. 
    async with request.app['mysql_db'].acquire() as conn:
        sql = '''
            SELECT a.r_code, b.r_code
            FROM annotation_type a, upstream_dc b
            WHERE a.upstream_dc_id = b.id AND a.id = $anno_cluster_id;
        '''
        sql = sql.replace("$anno_cluster_id", str(data["anno_cluster_id"]))
        res = await conn.execute(sql)
        row = await res.fetchone()
        anno_type_r_code = row[0]
        dc_r_code = row[1]

        if str(data["anno_subcluster_id"]) != '-':
            res1 = await conn.execute(annotation_subtype.select()
                .where(annotation_subtype.c.id == int(data["anno_subcluster_id"]))
            )
            row1 = await res1.fetchone()
            anno_subtype_r_code = row1["dc_r_code"] + '\n' + row1["anno_r_code"] + '\n'

        if str(data["combined_id"]) != '-':
            # get combined_r_code
            if str(data["combined_type"]) == 'merge':
                res2 = await conn.execute(upstream_merge.select()
                    .where(upstream_merge.c.id == int(data["combined_id"]))
                )
                row2 = await res2.fetchone()
                combined_r_code = row2["r_code"]
            else:
                res2 = await conn.execute(upstream_integrate.select()
                    .where(upstream_integrate.c.id == int(data["combined_id"]))
                )
                row2 = await res2.fetchone()
                combined_r_code = row2["r_code"]

            # get qc_r_code load_data_r_code
            # merge/integrate data are not generated from generate_loom result
            for x in str(data["combined_data_ids"]).split(':'):
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(x)))
                row3 = await res3.fetchone()
                right_species = row3['species']
                rbaseName = 'seurat_' + x
                if row3["data_type"] == "fastq":
                    res4 = await conn.execute(upstream_reads_mapping.select()
                        .where(upstream_reads_mapping.c.upload_data_id == row3["id"])
                    )
                    row4 = await res4.fetchone()
                    load_data_r_code += rbaseName+'.data <- Read10X("'+row4["result_path"]+'")\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res5 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.input_data_id == row4["id"])
                    )
                    row5 = await res5.fetchone()
                    qc_r_code += row5["r_code"]+"\n"

                elif row3["data_type"] == "cellranger":
                    load_data_r_code += rbaseName+'.data <- Read10X("'+row3["upload_path"]+'")\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res4 = await conn.execute(upstream_quality_control.select()
                            .where(upstream_quality_control.c.input_data_id == row3["id"])
                        )
                    row4 = await res4.fetchone()
                    qc_r_code += row4["r_code"] + "\n"

                else: # row3["data_type"] == "matrix"
                    load_data_r_code = rbaseName+'.data <- read.table(file="'+row3["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                    rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                    #
                    res4 = await conn.execute(upstream_quality_control.select()
                        .where(upstream_quality_control.c.input_data_id == row3["id"])
                    )
                    row4 = await res4.fetchone()
                    qc_r_code += row4["r_code"] + "\n"

        else: # str(data["combined_id"]) == '-':
            resx = await conn.execute(upload_data.select()
                .where(upload_data.c.id == int(data["data_id"]))
            )
            rowx = await resx.fetchone()
            right_species = rowx['species']
            # get qc_r_code
            res2 = await conn.execute(upstream_quality_control.select()
                .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
            )
            row2 = await res2.fetchone()
            qc_r_code = row2["r_code"]
            # get load_data_r_code
            rbaseName = 'seurat_' + str(data["data_id"])
            if str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) == "-":
                res3 = await conn.execute(upstream_reads_mapping.select().
                        where(upstream_reads_mapping.c.upload_data_id == int(data["reads_mapping_id"]))
                    )
                row3 = await res3.fetchone()
                load_data_r_code = rbaseName+'.data <- Read10X("'+row3["result_path"]+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
                
            elif str(data["data_type"]) == 'fastq' and str(data["generate_loom_id"]) != "-":
                res3 = await conn.execute(upstream_generate_loom.select()
                        .where(upstream_generate_loom.c.id == int(data["generate_loom_id"]))
                    )
                row3 = await res3.fetchone()
                loom_file = row3['result_path'] + '/cellranger.loom'
                load_data_r_code = rbaseName+' <- read.loom.matrices(file = "'+loom_file+'")\n'+\
                    rbaseName+' <- as.Seurat('+rbaseName+')\n'+\
                    rbaseName+'[["RNA"]] <- '+rbaseName+'[["spliced"]]\n'+\
                    'DefaultAssay('+rbaseName+') <- "RNA"\n'+\
                    rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                    rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

            elif str(data["data_type"]) == 'cellranger':
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                row3 = await res3.fetchone()
                load_data_r_code = rbaseName+'.data <- Read10X("'+row3["upload_path"]+'")\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'

            elif str(data["data_type"]) == 'matrix':
                res3 = await conn.execute(upload_data.select().where(upload_data.c.id == int(data["data_id"])))
                row3 = await res3.fetchone()
                load_data_r_code = rbaseName+'.data <- read.table(file="'+row3["upload_path"]+'", sep = "\t", header = TRUE, row.names = 1)\n'+\
                rbaseName+' <- CreateSeuratObject(counts='+rbaseName+'.data,project = "'+rbaseName+'",min.cells=3,min.features=200)\n'+\
                rbaseName+'[["percent.mt"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^MT-")\n'+\
                rbaseName+'[["percent.rb"]] <- PercentageFeatureSet('+rbaseName+', pattern = "^RP[SL]")\n'
            else:
                pass

    # 3. generate r_code
    to_loom_r_code = ''
    if str(data["anno_subcluster_id"]) == '-':
        if data["combined_type"] != '-':
            to_loom_r_code = 'DefaultAssay(project.combined) <- "RNA"\n'+\
            'project.combined.loom <- as.loom(project.combined, filename = "out.loom", verbose = F)\n'+\
            'project.combined.loom$close_all()\n'
        else:
            to_loom_r_code = 'DefaultAssay('+rbaseName+') <- "RNA"\n'+\
            rbaseName+'.loom <- as.loom('+rbaseName+', filename = "out.loom", verbose = F)\n'+\
            rbaseName+'.loom$close_all()\n'
    else:# data["anno_type"] == 'subcluster_annotation'
        to_loom_r_code = 'DefaultAssay(subcluster) <- "RNA"\n'+\
            'subcluster.loom <- as.loom(subcluster, filename = "out.loom", verbose = F)\n'+\
            'subcluster.loom$close_all()\n'
    
    sh_code = r'''
export PATH=/home/deng/miniconda3/envs/scenic/bin:$PATH
pyscenic grn --num_workers $threads --output adj.tsv --method grnboost2 out.loom $tf_fname
pyscenic ctx adj.tsv $featherFile --annotations_fname $motifsFile --expression_mtx_fname out.loom --mode "dask_multiprocessing" --output reg.csv --num_workers $threads --mask_dropouts
pyscenic aucell out.loom reg.csv --output out_scenic.loom --num_workers $threads
    '''
    py_code = r'''
import sys
import pandas as pd
import loompy as lp
from pyscenic.binarization import binarize
from pyscenic.cli.utils import load_signatures

f_pyscenic_output = sys.argv[1]
regulon_file = sys.argv[2]

def get_motif_logo(regulon):
    base_url = "http://motifcollections.aertlab.org/v9/logos/"
    for elem in regulon.context:
        if elem.endswith('.png'):
            return(base_url + elem)

regulons = load_signatures(regulon_file)
select_cols = [i.name for i in regulons if len(i.genes) >= 5]
txt_file = "regulons.txt"
fo2 = open(txt_file, 'w')
for i in regulons:
    if i.name in select_cols:
        motif = get_motif_logo(i)
        genes = "\t".join(i.genes)
        tf = "%s(%sg)" % (i.transcription_factor, len(i.genes))
        fo2.write("%s\t%s\t%s\n" % (tf, motif, genes.replace("\t",",")))
        
lf = lp.connect(f_pyscenic_output, mode='r+', validate=False)
auc_mtx = pd.DataFrame(lf.ca.RegulonsAUC, index=lf.ca.CellID)
auc_mtx = auc_mtx[select_cols]
auc_mtx.to_csv("AUCell.txt", sep='\t')
    '''
    heatmap_r_code = r'''
options(stringsAsFactors = F)
library(Seurat)
library(dplyr)
library(data.table)
library(SeuratWrappers)
library(SeuratDisk)
library(ggplot2)

motif_target <- fread('regulons.txt', header = F)
AUC <- read.table('AUCell.txt', header = TRUE)
colnames(AUC) <- motif_target$V1

grn <- CreateSeuratObject(counts = t(AUC), project = "scenic", assay = "AUC", min.cells = 0, min.features = 0)
DefaultAssay(grn) <- "AUC"
grn <- NormalizeData(grn)
grn <- ScaleData(grn, features = rownames(grn@assays$AUC@counts))

out.loom <- Connect("out.loom")
out.seurat <- as.Seurat(out.loom)
Idents(out.seurat) <- 'check_clusters'

grn$check_clusters <- out.seurat@active.ident
Idents(grn) <- 'check_clusters'
marker_AUC <- FindAllMarkers(grn, assay = 'AUC', slot = 'data', only.pos = T)
top5 <- marker_AUC %>% group_by(cluster) %>% top_n(n = 5, wt = avg_log2FC)

p <- DoHeatmap(grn, features = top5$gene, slot='counts',group.bar = T) +
    scale_fill_gradientn(name = "Normalized\nAUC", colors = c("navy", "white", "firebrick3")) +
    FontSize(axis.text.y.left = element_text(face = "bold.italic"))

ggsave('heatmap_top5.png',plot = p, width = 18,height = 9,dpi = 600)
    '''

    # 4. generate script file and submit slurm job 
    script = r_script_grn.replace("#loadDataRcode", load_data_r_code)
    script = script.replace("#qcRcode", qc_r_code)
    if data["combined_type"] != '-':
        script = script.replace("#combinedRcode", combined_r_code)
    script = script.replace("#dcRcode", dc_r_code)
    script = script.replace("#annotationTypeRcode", anno_type_r_code)
    if str(data["anno_subcluster_id"]) != '-':
        script = script.replace("#annotationSubtypeRcode", anno_subtype_r_code)
    script = script.replace("#grnRcode", to_loom_r_code)

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="grn",
        suffix=".R",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file:
        await file.write(script)

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="grn",
        suffix=".py",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file2:
        await file2.write(py_code)

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="grn2",
        suffix=".R",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file3:
        await file3.write(heatmap_r_code)

    baseDir = pathlib.Path(config["regulatoryResultPath"])
    userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    while userDir.exists():
        userDir = baseDir / "_".join(["user", str(data["user_id"]), 
                                            "".join(random.sample(string.ascii_letters + string.digits, 5))])
    await asyncio.to_thread(userDir.mkdir, parents=True)

    script1 = downstream_bash.replace("partition", config["slurmPartition"])
    script1 = script1.replace("jobLogPath", config["slurmJobLogPath"])
    script1 = script1.replace("downstreamCode", "/usr/bin/Rscript "+file.name)
    script1 = script1.replace("userDir", userDir.absolute().__str__())
    script1 = script1.replace("$localcores", str(data["threads"]))

    sh_code = sh_code.replace("$threads", str(data["threads"]))
    if right_species == 'human':
        sh_code = sh_code.replace("$tf_fname",
                                  config["grnResourceFilePath"]+"/allTFs_hg38.txt")
        sh_code = sh_code.replace("$featherFile", 
                                  config["grnResourceFilePath"]+
                                  "/hg38__refseq-r80__10kb_up_and_down_tss.mc9nr.genes_vs_motifs.rankings.feather")
        sh_code = sh_code.replace("$motifsFile",
                                  config["grnResourceFilePath"]+"/motifs-v9-nr.hgnc-m0.001-o0.0.tbl")
    else:
        sh_code = sh_code.replace("$tf_fname",
                                  config["grnResourceFilePath"]+"/allTFs_mm.txt")
        sh_code = sh_code.replace("$featherFile", 
                                  config["grnResourceFilePath"]+
                                  "/mm10_10kbp_up_10kbp_down_full_tx_v10_clust.genes_vs_motifs.rankings.feather")
        sh_code = sh_code.replace("$motifsFile",
                                  config["grnResourceFilePath"]+"/motifs-v9-nr.mgi-m0.001-o0.0.tbl")

    script1 += "\n" + sh_code + "\n"
    script1 += "python " + file2.name + " out_scenic.loom reg.csv\n"
    script1 += "/usr/bin/Rscript "+file3.name + "\n"

    async with aiofiles.tempfile.NamedTemporaryFile(
        mode="w",
        prefix="grn",
        suffix=".sh",
        dir=config['slurmJobScriptPath'],
        delete=False
    ) as file4:
        await file4.write(script1)

    """
    cmd = "sbatch "+file4.name
    proc = await asyncio.create_subprocess_shell(
        cmd, 
        stdout=asyncio.subprocess.PIPE, 
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    x = stdout.decode().split()
    jobID = int(x[-1])
    """

    result = await asyncio.to_thread(subprocess.check_output, ['/usr/local/bin/sbatch', str(file4.name)])
    output = result.decode('utf-8')
    x = output.split()
    jobID = int(x[-1])
    
    # 5. insert value to downstream table
    async with lock:
        slurm_jobs[jobID] = 'grn'

    if str(data["anno_subcluster_id"]) != "-":
        input_data_type = "annotation_subtype"
        annotation_id = int(data["anno_subcluster_id"])
    else:
        input_data_type = "annotation_type"
        annotation_id = int(data["anno_cluster_id"])

    async with request.app['mysql_db'].acquire() as conn:
        resx = await conn.execute(downstream.select()
                .where((downstream.c.annotation_id == annotation_id) &
                       (downstream.c.input_data_type == input_data_type) &
                       (downstream.c.analysis_type == "grn")
                )
            )
        rowx = await resx.fetchone()
        if rowx:
            # 删除原本的分群结果目录
            rmtree(str(rowx["result_path"]))
            # 更新数据库中的结果
            async with conn.begin() as transaction:
                await conn.execute(downstream.update()
                    .where((downstream.c.annotation_id == annotation_id) &
                        (downstream.c.input_data_type == input_data_type) &
                        (downstream.c.analysis_type == "grn")
                    )
                    .values(
                        slurm_job_id = jobID,
                        slurm_job_state = "PENDING",
                        result_path = userDir.absolute().__str__(),
                    )
                )
                await transaction.commit()
        else:
            async with conn.begin() as transaction:
                await conn.execute(downstream.insert().values(
                    user_id = int(data["user_id"]),
                    annotation_id = annotation_id,
                    input_data_type = input_data_type,
                    slurm_job_id = jobID,
                    slurm_job_state = "PENDING",
                    analysis_type = "grn",
                    result_path = userDir.absolute().__str__(),
                ))
                await transaction.commit() 

    return web.Response(text="OK")

####################################### management ###################################################
# 管理员可以取消、暂停自己以及别人提交的任务
# 用户只能取消、暂停自己提交的任务
async def query_slurm_job(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    results = []
    
    if str(data["is_admin"]) == "y":
        async with request.app['mysql_db'].acquire() as conn:
            res = await conn.execute("SELECT * FROM downstream;")
            rows = await res.fetchall()
            for row in rows:
                results.append({
                    "user_id": str(row["user_id"]),
                    "slurm_job_id": str(row["slurm_job_id"]),
                    "slurm_job_state": row["slurm_job_state"],
                    "analysis_type": row["analysis_type"],
                    "input_data_type": row["input_data_type"],
                    "data_id": str(row["annotation_id"])
                })

            sql = '''
                SELECT a.user_id, a.id, b.slurm_job_id, b.slurm_job_state
                FROM   upload_data a, upstream_reads_mapping b
                WHERE a.id = b.upload_data_id;
            '''
            res1 = await conn.execute(sql)
            rows1 = await res1.fetchall()
            for row1 in rows1:
                results.append({
                    "user_id": str(row1[0]),
                    "slurm_job_id": str(row1[2]),
                    "slurm_job_state": row1[3],
                    "analysis_type": "upstream_reads_mapping",
                    "input_data_type": "upload_data",
                    "data_id": str(row1[1])
                })
            
            sql1 = '''
            SELECT a.user_id, b.id, c.slurm_job_id, c.slurm_job_state
            FROM   upload_data a, upstream_reads_mapping b, upstream_generate_loom c
            WHERE a.id = b.upload_data_id AND b.id = c.upstream_reads_mapping_id;
            '''
            res2 = await conn.execute(sql1)
            rows2 = await res2.fetchall()
            for row2 in rows2:
                results.append({
                    "user_id": str(row2[0]),
                    "slurm_job_id": str(row2[2]),
                    "slurm_job_state": row2[3],
                    "analysis_type": "upstream_generate_loom",
                    "input_data_type": "upstream_reads_mapping",
                    "data_id": str(row2[1])
                })
    else:
        async with request.app['mysql_db'].acquire() as conn:
            res = await conn.execute(downstream.select()
                    .where(downstream.c.user_id == int(data["user_id"]))
                )
            rows = await res.fetchall()
            for row in rows:
                results.append({
                    "user_id": str(row["user_id"]),
                    "slurm_job_id": str(row["slurm_job_id"]),
                    "slurm_job_state": row["slurm_job_state"],
                    "analysis_type": row["analysis_type"],
                    "input_data_type": row["input_data_type"],
                    "data_id": str(row["annotation_id"])
                })

            sql = '''
                SELECT a.user_id, a.id, b.slurm_job_id, b.slurm_job_state
                FROM   upload_data a, upstream_reads_mapping b
                WHERE a.id = b.upload_data_id AND a.user_id = $user_id;
            '''
            sql = sql.replace("$user_id", str(data["user_id"]))
            res1 = await conn.execute(sql)
            rows1 = await res1.fetchall()
            for row1 in rows1:
                results.append({
                    "user_id": str(row1[0]),
                    "slurm_job_id": str(row1[2]),
                    "slurm_job_state": row1[3],
                    "analysis_type": "upstream_reads_mapping",
                    "input_data_type": "upload_data",
                    "data_id": str(row1[1])
                })
            
            sql1 = '''
            SELECT a.user_id, b.id, c.slurm_job_id, c.slurm_job_state
            FROM   upload_data a, upstream_reads_mapping b, upstream_generate_loom c
            WHERE a.id = b.upload_data_id AND b.id = c.upstream_reads_mapping_id AND a.user_id = $user_id;
            '''
            sql1 = sql1.replace("$user_id", str(data["user_id"]))
            res2 = await conn.execute(sql1)
            rows2 = await res2.fetchall()
            for row2 in rows2:
                results.append({
                    "user_id": str(rows2[0]),
                    "slurm_job_id": str(row2[2]),
                    "slurm_job_state": row2[3],
                    "analysis_type": "upstream_generate_loom",
                    "input_data_type": "upstream_reads_mapping",
                    "data_id": str(row2[1])
                })

    return web.json_response(results)

async def manage_slurm_job(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    slurm_job_id = str(data["slurm_job_id"])
    if str(data["cmd"]) == "cancel":
        result = await asyncio.to_thread(subprocess.check_output, ['/usr/local/bin/scancel', slurm_job_id]) 
        return web.Response(text = result.decode('utf-8'))
    elif str(data["cmd"]) == "resume":
        result = await asyncio.to_thread(subprocess.check_output, ['/usr/local/bin/scontrol', "resume", slurm_job_id]) 
        return web.Response(text = result.decode('utf-8'))
    else:  # str(data["cmd"]) == "suspend"
        result = await asyncio.to_thread(subprocess.check_output, ['/usr/local/bin/scontrol', "suspend", slurm_job_id]) 
        return web.Response(text = result.decode('utf-8'))

async def add_user(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    name = str(data["email"])
    email = str(data["email"])
    passwd = str(data["passwd"])

    async with request.app['mysql_db'].acquire() as conn:
        res = await conn.execute(user.select()
                .where(user.c.id == int(data["user_id"]))
            )
        row = await res.fetchone()
        if row["authority"] == "admin":
            async with conn.begin() as transaction:
                await conn.execute(user.insert().values(
                        name = name,
                        passwd = passwd,
                        email = email,
                        authority = 'user'
                    ))
                await transaction.commit()

            return web.Response(text = "OK")
        else:
            return web.Response(text = "Permission denied!")

####################################### download analysis result ######################################

# 上游与注释结果按照之前的 query_downstream_data 
# 下游分析结果 从 downstream 表中查询
# 这个函数只返回 downstream 表的结果
async def query_analysis_result(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    results = []
    async with request.app['mysql_db'].acquire() as conn:
        sql = '''
            SELECT * FROM downstream a
            WHERE a.user_id = $user_id AND a.slurm_job_state = 'COMPLETED'
            ORDER BY a.input_data_type;
        '''
        sql = sql.replace("$user_id", str(data["user_id"]))
        res = await conn.execute(sql)
        rows = await res.fetchall()
        # get all annotation_type & annotation_id
        all_ids = {}
        for row in rows:
            all_ids[str(row["input_data_type"])+":"+str(row["annotation_id"])] = 0

        for key in all_ids:
            is_inferCNV = "-"
            is_correlation = "-"
            is_cellCycleScore = "-"
            is_cellFrequency = "-"
            is_GO_KEGG_enrichment = "-"
            is_GSEA = "-"
            is_GSVA = "-"
            is_velocity = "-"
            is_trajectory = "-"
            is_cellCommunication = "-"
            is_GRN = "-"
            annotation_id = int(str(key).split(":")[1])
            annotation_type = str(key).split(":")[0]

            for row in rows:
                if (str(row["input_data_type"])+":"+str(row["annotation_id"]) == key 
                    and str(row["analysis_type"]) == "inferCNV") :
                    is_inferCNV = "Y"
                if (str(row["input_data_type"])+":"+str(row["annotation_id"]) == key 
                    and str(row["analysis_type"]) == "correlation") :
                    is_correlation = "Y"
                if (str(row["input_data_type"])+":"+str(row["annotation_id"]) == key 
                    and str(row["analysis_type"]) == "cellcycle") :
                    is_cellCycleScore = "Y"
                if (str(row["input_data_type"])+":"+str(row["annotation_id"]) == key 
                    and str(row["analysis_type"]) == "cell_frequency") :
                    is_cellFrequency = "Y"
                if (str(row["input_data_type"])+":"+str(row["annotation_id"]) == key 
                    and str(row["analysis_type"]) == "enrichment") :
                    is_GO_KEGG_enrichment = "Y"
                if (str(row["input_data_type"])+":"+str(row["annotation_id"]) == key 
                    and str(row["analysis_type"]) == "gsea") :
                    is_GSEA = "Y"
                if (str(row["input_data_type"])+":"+str(row["annotation_id"]) == key 
                    and str(row["analysis_type"]) == "gsva") :
                    is_GSVA = "Y"
                if (str(row["input_data_type"])+":"+str(row["annotation_id"]) == key 
                    and str(row["analysis_type"]) == "velocity") :
                    is_velocity = "Y"
                if (str(row["input_data_type"])+":"+str(row["annotation_id"]) == key 
                    and str(row["analysis_type"]) == "trajectory") :
                    is_trajectory = "Y"
                if (str(row["input_data_type"])+":"+str(row["annotation_id"]) == key 
                    and str(row["analysis_type"]) == "cellphonedb") :
                    is_cellCommunication = "Y"
                if (str(row["input_data_type"])+":"+str(row["annotation_id"]) == key 
                    and str(row["analysis_type"]) == "grn") :
                    is_GRN = "Y"

            results.append({
                "annotation_id": str(annotation_id),
                "annotation_type": annotation_type,
                "is_inferCNV": is_inferCNV,
                "is_correlation": is_correlation,
                "is_cellCycleScore": is_cellCycleScore,
                "is_cellFrequency": is_cellFrequency,
                "is_GO_KEGG_enrichment": is_GO_KEGG_enrichment,
                "is_GSEA": is_GSEA,
                "is_GSVA": is_GSVA,
                "is_velocity": is_velocity,
                "is_trajectory": is_trajectory,
                "is_cellCommunication": is_cellCommunication,
                "is_GRN": is_GRN
            })

    return web.json_response(results)

# 需要annotation_type annotation_id
async def view_download(request):
    data = await request.post()
    async with lock_user:
        if data["user_id"] not in logined_user:
            return web.Response(text="not login!", status=401)
        
        if data["token"] != logined_user[data["user_id"]]:
            return web.Response(text="token error!", status=401)
    
    # 1. 生成临时目录
    # 2. 将结果图片复制到临时目录
    # 3. 在/tmp目录, 将目录打包压缩为 .zip 文件
    # 4. 返回文件
    async with request.app['mysql_db'].acquire() as conn:
        # 1 生成临时目录
        result_dir = pathlib.Path("/tmp/"+ "user_" + str(data["user_id"])+"_"+
                                str(data["annotation_type"])+"_"+
                                str(data["annotation_id"])+"_"+
                                "analysis_result")

        ups= pathlib.Path(result_dir) / "upstream_results"
        anno_type = pathlib.Path(result_dir) / "annotation_cluster_results"
        if data["annotation_type"] == "annotation_subtype":
            anno_subtype = pathlib.Path(result_dir) / "annotation_subcluster_results"
            await asyncio.to_thread(anno_subtype.mkdir, parents=True, exist_ok=True)
        downs = pathlib.Path(result_dir) / "downstream_results"

        await asyncio.to_thread(ups.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(anno_type.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(downs.mkdir, parents=True, exist_ok=True)

        # 2. 将结果图片复制到临时目录
        # 2.1 上游分析结果
        if str(data["combined_id"]) != "-":
            res = await conn.execute(upstream_dc.select()
                .where(upstream_dc.c.id == int(data["dc_id"]))
            )
            row = await res.fetchone()
            # get .png file path using glob
            dc_path = await asyncio.to_thread(glob.glob, row["result_path"]+ "/*.png")
            await asyncio.to_thread(subprocess.run, ["/usr/bin/cp"] + dc_path + [str(ups)])
        else:
            res0 = await conn.execute(upstream_quality_control.select()
                .where(upstream_quality_control.c.id == int(data["quality_control_id"]))
            )
            row0 = await res0.fetchone()
            qc_path = await asyncio.to_thread(glob.glob, row0["result_path"]+ "/*.png")

            res1 = await conn.execute(upstream_dc.select()
                .where(upstream_dc.c.id == int(data["dc_id"]))
            )
            row1 = await res1.fetchone()
            dc_path = await asyncio.to_thread(glob.glob, row1["result_path"]+ "/*.png")

            await asyncio.to_thread(subprocess.run, ["/usr/bin/cp"] + qc_path + [str(ups)])
            await asyncio.to_thread(subprocess.run, ["/usr/bin/cp"] + dc_path + [str(ups)])

        # 2.2 细胞身份注释结果
        res2 = await conn.execute(annotation_type.select()
                .where(annotation_type.c.id == int(data["anno_cluster_id"]))
            )
        row2 = await res2.fetchone()
        anno_path = await asyncio.to_thread(glob.glob, row2["result_path"]+ "/*.png")
        await asyncio.to_thread(subprocess.run, ["/usr/bin/cp"] + anno_path + [str(anno_type)])

        if str(data["anno_subcluster_id"]) != "-":
            res3 = await conn.execute(annotation_subtype.select()
                .where(annotation_subtype.c.id == int(data["anno_subcluster_id"]))
            )
            row3 = await res3.fetchone()
            subtype_path = await asyncio.to_thread(glob.glob, row3["result_path"]+ "/*.png")
            await asyncio.to_thread(subprocess.run, ["/usr/bin/cp"] + subtype_path + [str(anno_subtype)])

        # 2.3 下游分析结果结果
        ress = await conn.execute(downstream.select()
            .where(
                (downstream.c.input_data_type == str(data["annotation_type"])) &
                (downstream.c.annotation_id == int(data["annotation_id"])) &
                (downstream.c.slurm_job_state == "COMPLETED")
            )
        )
        rowss = await ress.fetchall()
        for rowx in rowss:
            if str(rowx["analysis_type"]) == "gsea":
                gsea_path = await asyncio.to_thread(glob.glob, str(rowx["result_path"]) + "/*/*.png")
                await asyncio.to_thread(subprocess.run, ["/usr/bin/cp"] + gsea_path +[str(downs)])
            else:
                result_path = await asyncio.to_thread(glob.glob, str(rowx["result_path"]) + "/*.png")
                await asyncio.to_thread(subprocess.run, ["/usr/bin/cp"]+ result_path + [str(downs)])
    
        # 3 在/tmp目录, 将目录打包压缩为 .zip 文件
        outfile = "_".join(["user", str(data["user_id"]), str(data["annotation_type"]),
                str(data["annotation_id"]), "".join(random.sample(string.ascii_letters + string.digits, 5))])
        outfile += ".zip"

        await asyncio.to_thread(subprocess.run, ["/usr/bin/zip", "-r", "-q", "/tmp/"+outfile, result_dir])

        filepath = "/tmp/"+outfile

        filename = outfile
        async with aiofiles.open(filepath, 'rb') as f:
            file_data = await f.read()
            await f.close()
        headers = {
            'Content-Disposition': f'attachment; filename="{filename}"',
            'Content-Type': 'application/zip',
        }
        return web.Response(body=file_data, headers=headers)
