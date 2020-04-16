Thin Redis Object Layer (TROL)
==============================
**A library for light and predictable python object mappings to Redis**

.. image:: https://readthedocs.org/projects/redis-thin-object-layer/badge/?version=latest
   :target: https://redis-thin-object-layer.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

Documentation
-------------
Documentation is generated through sphinx and hosted at `Read the Docs`_! 

.. _Read the Docs: http://redis-thin-object-layer.readthedocs.io/en/latest/

Why did you build this?
-----------------------
I wanted an object oriented way to interact with redis that would provide exacting control over database layout,
predicatble and fast queries, and good documentation. (hopefully I got that last one right, but I'm not the one to
judge)

The first goal of trol is a statically defined, human-readble database structure defined by python classes. This allows
the dev to look at the database at runtime and read it as easily as the code which defined it. The dev should be able to
modify the database and know exactly what effect it will have on the program. As a result of this, trol explicitly does
not provide indexing or store supporting datastructures not defined by the programer.

The second goal of trol is fast and predictable querying. Any python access, function, or modification should result and
in one or zero network transfers. One result of this is a structure which encourages the dev to create a database where
eveything is defined in location and uniquely identifieable without searching.

.. include-in-docs-after-this-point

Getting started
---------------
``pip install trol`` and start defining your schema::

  >>> import trol
  >>> import redis
  ...
  >>> class MyDatabase(trol.Database):
  ...   redis = redis.Redis()
  ...
  ...   favorite_breweries = trol.SortedSet('favbreweries', typ=trol.Model)
  ...  
  ...   class Brewery(trol.Model):
  ...     def __init__(self, short_name):
  ...       self.id = short_name
  ...
  ...     location = trol.Property()
  ...     name = trol.Property(typ=str)
  ...     beers = trol.Set(typ=trol.Model)
  ...
  ...   class Beer(trol.Model):
  ...     def __init__(self, name, batch_number):
  ...       self.name = name
  ...       self.batch_number = batch_number
  ...
  ...     @property
  ...     def id(self):
  ...       return self.name + '@' + str(self.batch_number)
  ...
  ...     style = trol.Property()
  ...     rating = trol.Property(typ=int)
  ...
  >>> brewery = MyDatabase.Brewery('frmt')
  >>> brewery.location = (47.6490476, -122.3467747)
  >>> brewery.name = "Fremont Brewing Company"
  >>> lush = MyDatabase.Beer('Lush IPA', 120)
  >>> lush.style = "Indian Pale Ale"
  >>> lush.rating = 5
  >>> universale = MyDatabase.Beer('Universale', 245)
  >>> universale.style = "American Pale Ale"
  >>> universale.rating = 5
  >>> brewery.beers.add(lush, universale)
  2
  >>> MyDatabase.favorite_breweries.add(brewery, 10)
  1
  >>> set(MyDatabase.redis.keys()) == {
  ...   b'favbreweries',
  ...   b'Brewery:frmt:name',
  ...   b'Brewery:frmt:location',
  ...   b'Brewery:frmt:beers',
  ...   b'Beer:Lush IPA@120:style',
  ...   b'Beer:Lush IPA@120:rating',
  ...   b'Beer:Universale@245:style',
  ...   b'Beer:Universale@245:rating'
  ... }
  True
