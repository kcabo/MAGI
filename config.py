import os

class Base:
    SQLALCHEMY_DATABASE_URI = os.environ['DATABASE_URL']
    SQLALCHEMY_TRACK_MODIFICATIONS = False #これ書かないとログがうるさくなる
    TESTING = True

class Develop(Base):
    DEBUG = True

class Product(Base):
    DEBUG = False
