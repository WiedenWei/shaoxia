from aiohttp import web
from aiomysql.sa import create_engine
from routes import setup_routes
from settings import config
from views import query_slurm_job_state
import asyncio
import argparse
import os
import time

async def create_aiomysql(app):
    app['mysql_db'] = await create_engine(
        user=str(config["MysqlUser"]),
        db=str(config["MysqlDatabase"]),
        host=str(config["MysqlHost"]),
        password=str(config["MysqlPassword"]),
    )

# slurm mysql database
async def create_slurm_database(app):
    app['slurm_db'] = await create_engine(
        user=str(config["slurmUser"]),
        db=str(config["slurmDatabase"]),
        host=str(config["slurmHost"]),
        password=str(config["slurmPassword"]),
    )

async def delete_temp_png_file():
    while True:
        await asyncio.sleep(10*60)
        path = config["tempImageDir"]
        now = time.time()
        for file in os.listdir(path):
            file_path = os.path.join(path, file)
            if os.stat(file_path).st_mtime < now - 86400:
                if os.path.isfile(file_path):
                    os.remove(file_path)

async def delete_files(app):
    app['delete_files'] = asyncio.create_task(delete_temp_png_file())

async def close_delete_file(app):
    app['delete_files'].cancel()
    try:
        await app['delete_files']
    except asyncio.CancelledError:
        print("delete_files(): cancel_me is cancelled now")

async def dispose_aiomysql(app):
    app['mysql_db'].close()
    await app['mysql_db'].wait_closed()

async def dispose_slurmdb(app):
    app['slurm_db'].close()
    await app['slurm_db'].wait_closed()

async def infinite_loop(app):
    app['query_slurm'] = asyncio.create_task(query_slurm_job_state(app))

async def close_loop(app):
    app['query_slurm'].cancel()
    try:
        await app['query_slurm']
    except asyncio.CancelledError:
        print("query_slurm(): cancel_me is cancelled now")
    

parser = argparse.ArgumentParser(description="aiohttp server shaoxiao")
parser.add_argument('--path')
parser.add_argument('--port')
args = parser.parse_args()

app = web.Application(client_max_size=1024**3*50) # client max size set to 50G for large file upload
app.on_startup.append(create_aiomysql)
app.on_startup.append(infinite_loop)
app.on_startup.append(create_slurm_database)
app.on_startup.append(delete_files)
app.on_cleanup.append(close_delete_file)
app.on_cleanup.append(dispose_aiomysql)
app.on_cleanup.append(close_loop)
app.on_cleanup.append(dispose_slurmdb)
setup_routes(app)

web.run_app(app, path=args.path, port=args.port)