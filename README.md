txredis
=======

Asynchronous Redis client for Twisted.

Usage
=====


	from twisted.internet.address import IPv4Address
	from txredis.client import RedisConnectionPool

	redis = (RedisConnectionPool(
		IPv4Address('TCP', '127.0.0.1', 6312),
		db=1,
		poolsize=10,
		password=None
	))

	deferred = redis.get('foo')
