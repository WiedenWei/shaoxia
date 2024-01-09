import aiofiles
import asyncio

async def main():
    async with aiofiles.tempfile.NamedTemporaryFile(mode='w', prefix='rna_mapping', suffix='.sh', delete=False) as file:
        await file.write("hello, world!")
    
    print(file.name)

if __name__ == '__main__':
    asyncio.run(main())