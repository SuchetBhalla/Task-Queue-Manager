# modules
from fastapi import FastAPI, Request, HTTPException, status, BackgroundTasks
from logging.handlers import QueueListener, QueueHandler
from pydantic import BaseModel, Field, ValidationError
from contextlib import asynccontextmanager
from fastapi.responses import Response
import background.worker as bgd
from signal import SIGINT
from typing import Union
import psycopg_pool
import psycopg
import logging
import asyncio
import secrets
import orjson
import queue
import os

# function definitions for this API's "lifespan" events
# START: LOGGER
def awaken_logger():
    """
    desc.: This function causes logging to be performed asynchronously.
    - QueueHandler: enqueues a log into the queue. This runs in the main thread.
    - QueueListener: dequeues the log and performs I/O. This runs in a separate thread, hence
    it does not block the main thread.
    reference: https://docs.python.org/3/library/logging.handlers.html
    """

    # init
    log_dir = '/backend/logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # main
    que= queue.Queue()

    file= logging.FileHandler(filename= os.path.join(log_dir, 'backend.log'), mode= 'a', encoding= 'utf-8')
    #refer: https://docs.python.org/3/library/logging.html#logrecord-attributes
    formatter= logging.Formatter('%(levelname)s: %(name)s: %(message)s')
    file.setFormatter( formatter )

    consumer= QueueListener(que, file)
    producer = QueueHandler(que)

    logger= logging.getLogger()
    logger.setLevel( logging.INFO )
    logging.getLogger('psycopg.pool').setLevel( logging.WARNING )
    logger.addHandler( producer )
    # do not propagate a child-logger's logs to his parent-logger
    logger.propagate= False

    consumer.start()

    return (logger, producer, consumer)

def repose_logger(logger : logging.getLogger, consumer : QueueListener):
    # desc.: [gracefully-] terminates the QueueListener
    logger.info('API: Shutdown\n<------->')
    consumer.stop()
# END: LOGGER

# START: DB
def get_db_cred():
    # Desc.: creates the credential-string from the environment-variables

    # 1. read the 'POSTGRES_PASSWORD' from the file at the path 'POSTGRES_PASWORD_FILE'
    pswd= None
    path= os.getenv('POSTGRES_PASSWORD_FILE')
    if path is not None and os.path.exists(path):
        try:
            with open(path, 'r') as f:
                pswd= f.read().strip()
        except Exception as e:
            print((f'ERROR: Failed to read the secrets-file at path {path} => Cannot create the'
            f' credential-string. Details : {str(e)}'))
            return None

    # 2. create the credential-string
    cred= None
    if pswd is not None:
        user= os.getenv('POSTGRES_USER')
        dbname= os.getenv('POSTGRES_DB')
        cred = f"dbname={dbname} user={user} password={pswd} host=db port=5432"
    return cred

async def heartbeat():
    """
    desc.: periodically checks the "health" of the connnections in the pool.
    if this check fails, then this API is shutdown.
    """

    global POOL

    while True:
        LOGGER.debug('Heartbeat check on the database')
        await POOL.check()
        await asyncio.sleep(60)

async def reconnect_failed(pool : psycopg_pool.AsyncConnectionPool):
    # desc.: if the postgres server is unreachable, terminate this API

    LOGGER.critical('Database unavailable; initiating API-shutdown')

    """
    although the postgres-server is un-reachable, I must close the pool.
    terminating the API without it raises an Exception, which prevents a graceful shutdown.
    """
    await pool.close(timeout= 5)

    os.kill( os.getpid(), SIGINT)

async def awaken_db(cred : str, min_size : int, max_size : int):
    # desc.: creates a pool of connections to the postgres server, and creates the required tables

    global POOL, LOGGER

    # 1 create a pool of connections to the database
    try:
        POOL= psycopg_pool.AsyncConnectionPool(conninfo= cred,
            # detail: PostgreSQL forks 1 process per connection
            min_size= min_size, max_size= max_size,
            # threads to create & delete connections
            num_workers= max_size,
            #max_waiting= 1000, # queue size
            max_idle= 120, # 2 min
            timeout= 30, # tha max. time to serve a connection to a request

            # no callback before passing a connection to the client
            # refer: https://www.psycopg.org/psycopg3/docs/advanced/pool.html#connections-life-cycle
            check= None,
            open= False, # this value is mandatory

            # the pool tries to obtain connections within this time
            # if unable then a function is called (which sends SIGINT to the API)
            # refer: https://www.psycopg.org/psycopg3/docs/api/pool.html#psycopg-pool-connection-pool-implementations
            reconnect_timeout= 60, reconnect_failed= reconnect_failed)

        # open 'MIN_SIZE' connections [within 2 min]
        #refer: https://www.psycopg.org/psycopg3/docs/api/pool.html#psycopg_pool.AsyncConnectionPool.open
        await POOL.open(wait= True, timeout= 120)

        logging.info('The pool is ready')

    except psycopg.OperationalError as e:
        # refer: https://www.psycopg.org/psycopg3/docs/api/pool.html#pool-exceptions
        LOGGER.error(f'OperationalError: Failed to create the pool: DETAILS: {e}')
        raise # an excpetion must be re-raised to reach the server i.e., "uvicorn"
    except Exception as e:
        LOGGER.error(f'ServerError: Failed to create the pool: DETAILS: {e} ')
        raise

    # 2 create tables
    try:
        async with POOL.connection() as conn:
            async with conn.pipeline() as p, conn.cursor() as cur:
                await cur.execute("""
                CREATE TABLE IF NOT EXISTS api_register(
                id SMALLINT GENERATED ALWAYS AS IDENTITY UNIQUE,
                email VARCHAR(64) PRIMARY KEY,
                api_key BYTEA NOT NULL,
                ip_address INET NOT NULL,
                timestamp TIMESTAMP DEFAULT LOCALTIMESTAMP(0))""")

                await cur.execute("""
                CREATE TABLE IF NOT EXISTS api_LOGGER(
                id SMALLINT NOT NULL,
                timestamp TIMESTAMP DEFAULT LOCALTIMESTAMP(0),
                ip_address INET NOT NULL,
                FOREIGN KEY (id) REFERENCES api_register(id) ON DELETE CASCADE
                )
                """)
                #await p.sync()
            LOGGER.info('SUCCESS: The tables were either created or exist already')
    except psycopg.OperationalError as e:
        LOGGER.error(f'OperationalError: Failed to create tables : DETAILS: {e}')
        raise # re-raise the exception, to shutdown this API
    except Exception as e:
        LOGGER.error(f'ServerError: Failed to create tables: DETAILS: {e}')
        raise

async def repose_db():
    # desc.: closes the pool of connections
    global POOL, LOGGER

    LOGGER.info(orjson.dumps(POOL.get_stats()))

    """
    check: if the pool is available, *then* close it. it may have been closed earlier, by
    the funcion 'reconnect_failed'; if an error had occured [//view the function's description]
    """
    try:
        async with POOL.connection() as conn:
            pass
        # wait <= 10 min for all queued-requests to terminate
        await POOL.close( timeout= 600 )
        LOGGER.info('Closed database-connection')
    except psycopg_pool.PoolClosed:
        pass
    except Exception as e:
        LOGGER.error(f'Unexpected DatabaseError. Details : {str(e)}')
# END: DB

@asynccontextmanager
async def lifespan(app : FastAPI):

    """
    desc.: the launch of this API is preceded & succeeded by events, stated before
    & after 'yield', respectively.
    refer: https://fastapi.tiangolo.com/advanced/events/#lifespan-events
    """

    global LOGGER, POOL

    # startup 1
    (LOGGER, _, consumer)= awaken_logger()
    LOGGER.info('Booting')

    # startup 2
    cred= get_db_cred()
    min_size = int( os.getenv('POOL_SIZE_MIN',   '5') )
    max_size= int( os.getenv('POOL_SIZE_MAX', '13') )
    await awaken_db(cred, min_size, max_size)
    task= asyncio.create_task( heartbeat() )

    LOGGER.info('Rise & Shine!')

    yield

    # shutdown 2
    # refer: https://docs.python.org/3/library/asyncio-task.html#asyncio.Task.cancel
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
            pass
    await repose_db()

    # shutdown 1
    repose_logger(LOGGER, consumer)

# class definitions
"""These pydantic models are used to validate the presence, data-type & length of
the parameters passed in the request's body"""
class RegisterRequest(BaseModel):
    email   : str   = Field(max_length= 64)
    model_config= {'extra' : 'forbid'} # reject the request if more headers are sent

class TriggerRequest(RegisterRequest):
    # 64 hex characters == 32 bytes == 256 bits
    api_key : str = Field(min_length= 64, max_length= 64)
    input : list[int] = Field(max_length= 5)

# global variables
POOL: Union[psycopg_pool.AsyncConnectionPool, None] = None
LOGGER= None
app= FastAPI(lifespan= lifespan)

# function definitions
async def log_success(user_id : str, ip_address : str):
    """
    desc.: upon successful-authentication of a client, this function is called to record an entry,
    into the database.
    This shall be run as a "background-task", i.e., after sending a response.
    This decreases the latency of the endpoint 'trigger' by 2.4 msec.
    refer: https://fastapi.tiangolo.com/tutorial/background-tasks/#using-backgroundtasks

    future work: an optimization can be done. the database access can be made faster by
    buffering multiple-logs, then writing at once via a "pipeline".
    refer: https://www.psycopg.org/psycopg3/docs/advanced/pipeline.html
    """
    async with POOL.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("INSERT INTO api_LOGGER(id, ip_address) VALUES(%s, %s)",
            (user_id, ip_address, ), prepare= True)

# path operation functions aka endpoints
@app.get('/ping')
def ping():
    # desc: to check if this API is live
    return Response(content= 'pong', status_code= 200, media_type= 'text/plain')

@app.api_route('/register', methods= ['GET', 'POST'])
async def register(request : Request):

    # desc: to register an email [with this API]
    global POOL, LOGGER

    if request.method == 'GET':
        return Response(content= ("To register your email, send a json request which contains"
        " your email ID. Set the key as 'email'."), media_type= 'text/plain', status_code= 200)

    elif request.method == 'POST':

        binary = await request.body()
        # error checks
        ## was a 'body' sent?
        if len(binary) < 1:
            raise HTTPException(status_code= 400,
                detail= 'ERROR: the request lacks a body')

        ## convert bytes to json
        try:
            data= orjson.loads(  binary.decode('utf-8') )
        except orjson.JSONDecodeError:
            raise HTTPException(status_code= 400,
            detail= ('The json-body is malformed. Cannot decode it.'))

        ## the pydantic-model validates the received-json
        try:
            body= RegisterRequest(**data)
        except ValidationError:
            raise HTTPException(status_code= 400,
            detail= ('The request must (only) contain an \'email\', '
            'which should be a string of <= 64 characters.'))

        # register
        # get the key 'email'
        email= body.email
        try:
            async with POOL.connection() as conn:
                async with conn.cursor() as cur:

                    # error check: is the email already registered?
                    await cur.execute("SELECT email FROM api_register WHERE email = %s",
                    (email,), prepare= True)
                    record= await cur.fetchone()
                    if record is not None:
                        raise HTTPException(status_code= 400,
                        detail= 'ERROR: This email is already registered.')

                    # generate an api_key with 256 bits
                    api_key= secrets.token_bytes(32)

                    """
                    read the client's ip-address
                    since I know that this API runs behind 1 proxy, I removede the following check.
                    # check
                    ip_address= request.headers.get('x-real-ip', None)
                    if ip_address is None:
                        ip_address = request.client.host
                    else:
                        ip_address = ip_address.split(',')[0].strip()
                    """
                    ip_address = request.headers.get('x-real-ip')

                    await cur.execute("""
                    INSERT INTO api_register(email, api_key, ip_address)
                    VALUES(%s, %s, %s)
                    """, (email, api_key, ip_address))

                    return Response(content= orjson.dumps({'api_key' : api_key.hex()}),
                    status_code= 200, media_type= 'application/json')

        except HTTPException:
            """
            to *propagate* the preceeding 'HTTPException' (placed the try block) to the
            [FastAPI-] app, I must [catch &] re-raise it. else it shall be caught by the following
            except-block i.e., 'except Exception', because the class 'HTTPException'
            inherits fromthe class 'Exception'
            """
            raise
        except psycopg.OperationalError as e:
            LOGGER.error( f'OperationalError: in endpoint \'register\' : Details : {str(e)}' )
            raise HTTPException(status_code= 500)
        except Exception as e:
            LOGGER.error( f'ServerError: in endpoint \'register\' : Details : {str(e)}' )
            raise HTTPException(status_code= 500)
    else:
        raise HTTPException(status_code= 405,
        detail= 'ERROR: Invalid request. Supported methods are POST & GET.')

@app.api_route('/trigger', methods= ['GET', 'POST'])
async def trigger(request : Request, background_tasks : BackgroundTasks):
    # desc: to [validate &] enqueue tasks to the redis-server

    global POOL, LOGGER

    if request.method == 'GET':
        return Response(content= 'To trigger a model, you must provide your email, api_key & input.',
        media_type= 'text/plain', status_code= 200)

    elif request.method == 'POST':

        binary = await request.body()
        # error checks
        ## was a 'body' sent?
        if len(binary) < 1:
            raise HTTPException(status_code= 400,
            detail= "ERROR: POST request requires an email & api_key.")

        ## convert bytes to json
        try:
            data= orjson.loads(  binary.decode('utf-8') )
        except orjson.JSONDecodeError:
            raise HTTPException(status_code= 400,
            detail= ('The json-body is malformed. Cannot decode it.'))

        ## the pydantic-model validates the received-json
        try:
            body= TriggerRequest(**data)
        except ValidationError:
            raise HTTPException(status_code= 400,
            detail= ('The request must (only) contain the headers:  \'email\', \'api_key\', \'input\'. '
            'data-types must be : (str, str, list[int]). Lengths must be (<= 64, == 64, <= 5'))

        ## is the 'api_key' plauisible?
        try:
            api_key= bytes.fromhex(body.api_key)
        except ValueError:
            raise HTTPException(status_code= 401,
            detail= "ERROR: impossible api_key. A valid key would contain hexadecimal characters.")

        # trigger
        email= body.email
        try:
            async with POOL.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        SELECT id, email, api_key FROM api_register
                        WHERE email = %s""" , (email, ),
                        prepare= True)

                    client_data= await cur.fetchone()
                    # error check: is this email registered, i.e., does it exist in the table 'api_register'?
                    if client_data is None:
                        raise HTTPException(status_code= status.HTTP_401_UNAUTHORIZED,
                        detail= "NOT FOUND: email not in database")

                    # validate the client's key AND intercept the 'timing attack'
                    # refer: https://docs.python.org/3/library/secrets.html#secrets.compare_digest
                    if secrets.compare_digest(client_data[2], api_key):
                        # enqueue the client's request
                        bgd.execute.send(email, body.input)
                    else:
                        raise HTTPException(status_code= status.HTTP_401_UNAUTHORIZED,
                        detail= 'ERROR: invalid api_key.')

                    """
                    the function 'log_sucess' shall run after returning the response; this decreases
                    the latency by 2.3 msec
                    refer: https://fastapi.tiangolo.com/tutorial/background-tasks/#using-backgroundtasks
                    """
                    background_tasks.add_task(log_success, client_data[0], request.headers.get('x-real-ip'))

                    return Response('SUCCESS: task enqueued.', status_code= 202,
                    media_type= 'text/plain')

        except HTTPException:
            raise # re-raise
        except psycopg.OperationalError as e:
            LOGGER.error( f'OperationalError : in endpoint \'trigger\' : Details : {str(e)}' )
            raise HTTPException(status_code= 500)
        except Exception as e:
            LOGGER.error( f'ServerError: in endpoint \'trigger\' : Details : {str(e)}')
            raise HTTPException(status_code= 500)
    else:
        raise HTTPException(status_code= 405,
        detail= 'ERROR: Invalid request. Supported methods are POST & GET.')
