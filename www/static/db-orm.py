
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
            host=kw.get('host','localhost'),
            port=kw.get('port',3306),
            user=kw['user'],
            password=kw['password'],
            db=kw['db'],
            charset=kw.get('charset','utf8'),
            autocommit=kw.get('autocommit',True),
            minsize=kw.get('minsize',1),
            loop=loop
            )


async def selet(sql,args,size=None):
    log(sel,args)
    global __pool
    with await __poop as conn:
        cur=await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?','%s'),args or ())
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs=await cur.fetchall()
        await cur.close()
        logging.info('rows returned %s'%len(rs))
        return rs
async def execute(sql,args):
    log(sql)
    with await __pool as conn:
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?','%s'),args)
            affected=cur.rowcount
            await cur.close()
        except BaseException as e:
            raise
        return affected



class User(Model):
    __table__ = 'users'
    id = IntegerField(primary_key=True)
    name = StringField()



class Filed(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name=name
        self.column_type=column_type
        self.primary_key=primary_key
        self.default=default

    def __str__(self):
        return '<%s, %s:%s>'%(self.__class__.__name__, self.column_type, self.name)

class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, dd1='varchar(100)'):
        super().__init(name,dd1,primary_key,default)
class BooleanField(Field):
    def __init__(self, name=None, primary_key=False, default=False):
        super().__init__(name, 'boolean', primary_key, default)
class IntegerField(Field):
    def __init__(self, name=None,primary_key=False, default=0):
        super().__init(name,'bigint',primary_key, default)
class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real',primary_key, default)
class TextField(Field):
    def __init(self, name=None, primary_key=False,default=None):
        super()__init__(name,'text', primary_key, default)


class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
        tableName = attrs.get('__table__',None) or name
        logging.info('found model: %s (table: %s)'%(name, tableName))
        mappings = dict()
        fields=[]
        primaryKey=None
        for k,v in attrs.items():
            if isinstance(v, Field):
                logging.info(' found mapping: %s ==> %s'%(k,v))
                mappings[k]=v
                if v.primary_key:
                    if primaryKey:
                        raise StandardError('Duplicate primary key for field: %s' %k)
                    primaryKey=k
                else:
                    fields.append(k)
        if not primaryKey:
            raise StandardError('Primary key not found.')
        for k in mapplings.keys():
            attrs.pop(k)
        escaped_fields=list(map(lambda f:'%s'%f,fields))
        attrs['__mappings__']=mappings
        attrs['__table__']=tableName
        attrs['__primary_key__']=primaryKey
        attrs['__fields__']=fields
        attrs['__select__']='select %s,%s from %s'%(primaryKey, ','.join(escaped_fields), tableName)
        attrs['__insert__']='insert into %s (%s,%s) values(%s)'%(tableName, ','.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields)+1))
        attrs['__update__']='update %s set $s where %s=?'%(tableName, ','.join(map(lambda f:'%s=?'%(mappings.get(f).name or f),fields)),primaryKey)
        attrs['__delete__']='delete from %s where %s=?'%(tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r'"Model" object has no attribute "%s"'%key)
    def __setattr__(self, key, value):
        self[key]=value
    def getValue(self, key):
        return getattr(self, key, None)
    def getValueOrDefault(self, key):
        value=getattr(self, key, None)
        if value is None:
            field=self.default() if callable(field.default) else field.default
            logging.debug('using default value for %s: %s'%(key, str(value)))
            setattr(self, key, value)
        return value


