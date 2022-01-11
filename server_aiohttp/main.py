import asyncio
import aioredis
from aiohttp import web
import yaml
from user_db import postgres_db
from pydantic import BaseModel, EmailStr, StrictStr, ValidationError, root_validator
from aiohttp_pydantic import PydanticView
import json, base64
import aiohttp_session
from aiohttp_session.redis_storage import RedisStorage

routes = web.RouteTableDef()


class userAlreadyExist(Exception):
    pass


def convert_password(cls, values):
    values['password'] = base64.b64encode((values['password'] + 'salt').encode('utf-8'))
    return values


class User(BaseModel):
    email: EmailStr
    password: StrictStr
    method: StrictStr
    device_id: StrictStr
    family: StrictStr

    _password_convertation = root_validator(allow_reuse=True)(convert_password)


class Creditionals(BaseModel):
    email: EmailStr
    password: StrictStr

    _password_convertation = root_validator(allow_reuse=True)(convert_password)


def read_config(source):
    def open_config(function_to_decorate):
        async def wrapper(*args):
            with open(u'.\\user_db\config') as f:
                app = args[0]
                data_map = yaml.safe_load(f)
                database_conn_parametrs = data_map[source]
            await function_to_decorate(app, database_conn_parametrs)
        return wrapper
    return open_config


@read_config(source='postgres')
async def pg_context(*args):
    app = args[0]
    database_conn_parametrs = args[1]
    print(f'postgres: {database_conn_parametrs}')
    app['db'] = postgres_db.Connection(**database_conn_parametrs)
    await app['db'].connect()  # создаю пул соединений с pg

@read_config(source='redis')
async def redis_connect(*args):
    app = args[0]
    database_conn_parametrs = args[1]
    print(f'redis: {database_conn_parametrs}')
    app['redis'] = await aioredis.create_redis_pool((database_conn_parametrs['host'], database_conn_parametrs['port']))


@web.middleware
async def response(request, handler):
    response = await handler(request)
    response_dict = json.loads(f'{response.text}')
    if not response_dict.get('success'):
        raise web.HTTPInternalServerError(text=json.dumps(result_dict))
    else:
        return response


@routes.post('/register')
async def register(request):
    body = await request.json()
    success = True
    fields = []
    error_msgs = []
    session = await aiohttp_session.get_session(request)
    if session.get('email'):
        print(f'Пользователь уже добавлен - высылаю {session}')
        return web.Response(text=json.dumps({'success': True, 'fields': [], 'error_msgs': []}))
    try:
        user = User(**body)
        check_user = await app['db'].select('users', {'email': user.__dict__['email']})
        if not check_user:
            result = await app['db'].insert('users', user.__dict__)
            session = await aiohttp_session.new_session(request)
            session['email'] = user.__dict__['email']
        else:
            raise userAlreadyExist
    except ValidationError as err:
        success = False
        [error_msgs.append(error_msg.get('msg')) for error_msg in err.errors()]
        [fields.append(*field) for field in [fields.get('loc') for fields in err.errors()]]
    except userAlreadyExist:
        success = False
        fields.append('email')
        error_msgs.append('User already exists')
    finally:
        return web.Response(text=json.dumps({'success': success, 'fields': fields, 'error_msgs': error_msgs}))


@routes.post('/login')
async def login(request):
    session = await aiohttp_session.get_session(request)
    print(request['aiohttp_session'])
    if session.get('email'):
        print(f'Сессия есть - высылаю {session}')
        return web.Response(text=json.dumps({'success': True, 'fields': [], 'error_msgs': []}))
    else:
        print(f'Сессии нет - создаю новую')
        body = await request.json()
        creditional = Creditionals(**body)
        result = await app['db'].select('users', {'email': creditional.__dict__['email']})
        print(result)
        if not result:
            return web.Response(text=json.dumps(
                {'success': False, 'fields': ['email', 'password'], 'error_msgs': ['invalid creditional']}))
        if result[0]['password'] == creditional.__dict__['password']:
            session = await aiohttp_session.new_session(request)
            session['email'] = creditional.__dict__['email']
            print(f'новая сессия {session}')
            return web.Response(text=json.dumps({'success': True, 'fields': [], 'error_msgs': []}))
        else:
            return web.Response(text=json.dumps(
                {'success': False, 'fields': ['email', 'password'], 'error_msgs': ['invalid creditional']}))


async def init(app):
    task_to_connect_pg = pg_context(app)
    task_to_connect_redis = redis_connect(app)
    await asyncio.gather(task_to_connect_redis, task_to_connect_pg)

async def main(app):
    await init(app)
    app.add_routes(routes)
    storage = RedisStorage(app['redis'], cookie_name="AIOHTTP_SESSION")
    aiohttp_session.setup(app, storage)
    return app

if __name__ == '__main__':
    app = web.Application(middlewares=[response])
    web.run_app(main(app))

