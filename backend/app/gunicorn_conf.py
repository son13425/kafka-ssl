from multiprocessing import cpu_count


bind = "127.0.0.1:9000"


workers = cpu_count() + 1
worker_class = 'uvicorn.workers.UvicornWorker'
