# -*- coding: UTF-8 -*-

import bisect
import functools
import operator
import re
import warnings
import zlib

from time import time
from itertools import imap, chain
from functools import partial
from types import DictType, ListType, TupleType
from collections import deque

try:
	import hiredis
except ImportError:
	hiredis = None

from twisted.internet import defer
from twisted.internet.defer import Deferred, DeferredLock, DeferredList, DeferredSemaphore
from twisted.internet.defer import DeferredQueue, inlineCallbacks, returnValue
from twisted.internet.defer import fail, succeed
from twisted.internet.protocol import ClientFactory, ReconnectingClientFactory, Protocol
from twisted.internet import reactor
from twisted.internet import task
from twisted.protocols.basic import LineReceiver
from twisted.protocols import policies
from twisted.python import log
from twisted.python.failure import Failure
from twisted.internet.address import IPv4Address, UNIXAddress


DEBUG = False
AUTH = False


class RedisError(Exception):
	pass


class ConnectionError(RedisError):
	pass


class ResponseError(RedisError):
	pass


class InvalidResponse(RedisError):
	pass


class InvalidData(RedisError):
	pass


class WatchError(RedisError):
	pass


def list_or_args(command, keys, args):
	oldapi = bool(args)
	try:
		iter(keys)
		if isinstance(keys, (str, unicode)):
			raise TypeError
	except TypeError:
		oldapi = True
		keys = [keys]

	if oldapi:
		warnings.warn(DeprecationWarning(
			"Passing *args to redis.%s is deprecated. "
			"Pass an iterable to ``keys`` instead" % command))
		keys.extend(args)
	return keys

def try_to_list(value):
	if not isinstance(value, (ListType, TupleType)):
		return (value, )

	return value

def try_to_numeric(value):
	try:
		return int(value) if value.find('.') == -1 else float(value)
	except (ValueError, AttributeError):
		return value


class Redis:

	def __init__(self, charset="utf-8", errors="strict"):
		self.charset = charset
		self.errors = errors

		self.replyQueueLength = 0
		self.replyQueue = defer.DeferredQueue()

		self.transactions = 0

		self.transaction = False
		self.transactionFree = False

		self.pipelining = False
		self.pipeliningFree = False
		self.pipeliningQueue = None

		self.coveredOnClient = False

		self.countErrors = 0
		self.maxErrors = 5

	def cancelCommands(self, reason):
		"""
		Cancel all the outstanding commands, making them fail with C{reason}.
		"""
		while self.replyQueueLength < 0:
			self.replyReceived(ConnectionError("Lost connection"))

	def replyReceived(self, reply):
		"""
		Complete reply received and ready to be pushed to the requesting
		function.
		"""

		if DEBUG:
			log.msg('Redis protocol reply', self, self.factory, repr(reply))

		if isinstance(reply, Exception):
			reply = Failure(reply)

			# Increment count errors
			self.countErrors += 1

		self.replyQueueLength += 1
		self.replyQueue.put(reply)

	def get_response(self):
		self.replyQueueLength -= 1
		return self.replyQueue.get()

	def encode(self, s):
		if isinstance(s, str):
			return s

		if isinstance(s, unicode):
			try:
				return s.encode(self.charset, self.errors)
			except UnicodeEncodeError, e:
				raise InvalidData("Error encoding unicode value '%s': %s" % (repr(s), e))

		if isinstance(s, float):
			return format(s, "f")

		return str(s)

	_b_command = "${0:d}\r\n{1}\r\n".format
	_m_command = "*{0:d}\r\n{1}".format

	def execute_command(self, *args):
		if DEBUG:
			if self.pipelining is False:
				log.msg('Redis protocol execute command', self, self.factory, args)
			else:
				log.msg('Redis protocol pipelining command', self, self.factory, args)

		if self.connected == 0:
			return fail(ConnectionError("Not connected"))

		"""
		cmds = [self._b_command(len(enc_value), enc_value) for enc_value in imap(self.encode, args)]
		data = (self._m_command(
			len(cmds),
			"".join(cmds)
		))
		"""

		cmds = (self._b_command(len(enc_value), enc_value) for enc_value in imap(self.encode, args))
		data = (self._m_command(
			len(args),
			"".join(cmds)
		))

		if self.pipelining is False:
			# Send to server
			deferred = self.get_response()
			self.transport.write(data)
		else:
			deferred = Deferred()
			self.pipeliningQueue.append((data, self.get_response(), deferred))

		return deferred

	def execute_command_p(self, *args, **kwargs):
		deferred = self.execute_command(*args, **kwargs)
		if self.pipelining is False:
			# Success
			return deferred

	# Connection handling
	def quit(self):
		"""
		Close the connection
		"""

		# Update last request time
		self._lastreq = time()

		return self.execute_command("QUIT")

	def auth(self, password):
		"""
		Simple password authentication if enabled
		"""

		# Update last request time
		self._lastreq = time()

		return self.execute_command("AUTH", password)

	def ping(self):
		"""
		Ping the server
		"""

		# Update last request time
		self._lastreq = time()

		return self.execute_command("PING")

	def unknown(self):
		"""
		Unknown command
		"""

		# Update last request time
		self._lastreq = time()

		return self.execute_command("UNKNOWN")

	def exists(self, key):
		"""
		Test if a key exists
		"""

		# Update last request time
		self._lastreq = time()

		return self.execute_command("EXISTS", key)

	def delete(self, *keys):
		"""
		Delete one or more keys
		"""

		# Update last request time
		self._lastreq = time()

		return self.execute_command("DEL", *keys)

	def type(self, key):
		"""
		Return the type of the value stored at key
		"""

		# Update last request time
		self._lastreq = time()

		return self.execute_command("TYPE", key)

	def keys(self, pattern="*"):
		"""
		Return all the keys matching a given pattern
		"""

		# Update last request time
		self._lastreq = time()

		return self.execute_command("KEYS", pattern)

	def randomkey(self):
		"""
		Return a random key from the key space
		"""

		# Update last request time
		self._lastreq = time()

		return self.execute_command("RANDOMKEY")

	def rename(self, oldkey, newkey):
		"""
		Rename the old key in the new one,
		destroying the newname key if it already exists
		"""

		# Update last request time
		self._lastreq = time()

		return self.execute_command("RENAME", oldkey, newkey)

	def renamenx(self, oldkey, newkey):
		"""
		Rename the oldname key to newname,
		if the newname key does not already exist
		"""

		# Update last request time
		self._lastreq = time()

		return self.execute_command("RENAMENX", oldkey, newkey)

	def dbsize(self):
		"""
		Return the number of keys in the current db
		"""

		# Update last request time
		self._lastreq = time()

		return self.execute_command("DBSIZE")

	def expire(self, key, seconds):
		"""
		Set a time to live in seconds on a key
		"""

		# Update last request time
		self._lastreq = time()

		return self.execute_command("EXPIRE", key, seconds)

	def persist(self, key):
		"""
		Remove the expire from a key
		"""

		# Update last request time
		self._lastreq = time()

		return self.execute_command("PERSIST", key)

	def ttl(self, key):
		"""
		Get the time to live in seconds of a key
		"""

		# Update last request time
		self._lastreq = time()

		return self.execute_command("TTL", key)

	def select(self, index):
		"""
		Select the DB with the specified index
		"""

		# Update last request time
		self._lastreq = time()

		return self.execute_command("SELECT", index)

	def move(self, key, dbindex):
		"""
		Move the key from the currently selected DB to the dbindex DB
		"""

		# Update last request time
		self._lastreq = time()

		return self.execute_command("MOVE", key, dbindex)

	def flushdb(self):
		"""
		Remove all the keys from the currently selected DB
		"""

		# Update last request time
		self._lastreq = time()

		return self.execute_command("FLUSHDB")

	def flushall(self):
		"""
		Remove all the keys from all the databases
		"""

		# Update last request time
		self._lastreq = time()

		return self.execute_command("FLUSHALL")

	def set(self, key, value):
		"""
		Set a key to a string value
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("SET", key, value)
		if self.pipelining is False:
			# Success
			return deferred

	def get(self, key):
		"""
		Return the string value of the key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("GET", key)
		if self.pipelining is False:
			# Success
			return deferred

	def getset(self, key, value):
		"""
		Set a key to a string returning the old value of the key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("GETSET", key, value)
		if self.pipelining is False:
			# Success
			return deferred

	def mget(self, keys):
		"""
		Multi-get, return the strings values of the keys
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("MGET", *keys)
		if self.pipelining is False:
			# Success
			return deferred

	def setnx(self, key, value):
		"""
		Set a key to a string value if the key does not exist
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("SETNX", key, value)
		if self.pipelining is False:
			# Success
			return deferred

	def setex(self, key, seconds, value):
		"""
		Set+Expire combo command
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("SETEX", key, seconds, value)
		if self.pipelining is False:
			# Success
			return deferred

	def mset(self, mapping):
		"""
		Set the respective fields to the respective values.
		"""

		# Update last request time
		self._lastreq = time()

		items = []
		for pair in mapping.iteritems():
			items.extend(pair)

		deferred = self.execute_command("MSET", *items)
		if self.pipelining is False:
			# Success
			return deferred

	def msetnx(self, mapping):
		"""
		Set multiple keys to multiple values in a single atomic
		operation if none of the keys already exist
		"""

		# Update last request time
		self._lastreq = time()

		items = []
		for pair in mapping.iteritems():
			items.extend(pair)

		deferred = self.execute_command("MSETNX", *items)
		if self.pipelining is False:
			# Success
			return deferred

	def incr(self, key):
		"""
		Increment the integer value of key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("INCR", key)
		if self.pipelining is False:
			# Success
			return deferred

	def incrby(self, key, amount):
		"""
		Increment the integer value of key by integer
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("INCRBY", key, amount)
		if self.pipelining is False:
			# Success
			return deferred

	def decr(self, key, amount=1):
		"""
		Decrement the integer value of key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("DECRBY", key, amount)
		if self.pipelining is False:
			# Success
			return deferred

	def decrby(self, key, amount):
		"""
		Decrement the integer value of key by integer
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.decr(key, amount)
		if self.pipelining is False:
			# Success
			return deferred

	def append(self, key, value):
		"""
		Append the specified string to the string stored at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("APPEND", key, value)
		if self.pipelining is False:
			# Success
			return deferred

	def substr(self, key, start, end=-1):
		"""
		Return a substring of a larger string
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("SUBSTR", key, start, end)
		if self.pipelining is False:
			# Success
			return deferred

	def rpush(self, key, value):
		"""
		Append an element to the tail of the List value at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("RPUSH", key, *try_to_list(value))
		if self.pipelining is False:
			# Success
			return deferred

	def lpush(self, key, value):
		"""
		Append an element to the head of the List value at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("LPUSH", key, value)
		if self.pipelining is False:
			# Success
			return deferred

	def llen(self, key):
		"""
		Return the length of the List value at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("LLEN", key)
		if self.pipelining is False:
			# Success
			return deferred

	def lrange(self, key, start, end):
		"""
		Return a range of elements from the List at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("LRANGE", key, start, end)
		if self.pipelining is False:
			# Success
			return deferred

	def ltrim(self, key, start, end):
		"""
		Trim the list at key to the specified range of elements
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("LTRIM", key, start, end)
		if self.pipelining is False:
			# Success
			return deferred

	def lindex(self, key, index):
		"""
		Return the element at index position from the List at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("LINDEX", key, index)
		if self.pipelining is False:
			# Success
			return deferred

	def lset(self, key, index, value):
		"""
		Set a new value as the element at index position of the List at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("LSET", key, index, value)
		if self.pipelining is False:
			# Success
			return deferred

	def lrem(self, key, count, value):
		"""
		Remove the first-N, last-N, or all the elements matching value
		from the List at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("LREM", key, count, value)
		if self.pipelining is False:
			# Success
			return deferred

	def lpop(self, key):
		"""
		Return and remove (atomically) the first element of the List at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("LPOP", key)
		if self.pipelining is False:
			# Success
			return deferred

	def rpop(self, key):
		"""
		Return and remove (atomically) the last element of the List at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("RPOP", key)
		if self.pipelining is False:
			# Success
			return deferred

	def blpop(self, keys, timeout=0):
		"""
		Blocking LPOP
		"""

		# Update last request time
		self._lastreq = time()

		if isinstance(keys, (str, unicode)):
			keys = [keys]
		else:
			keys = list(keys)

		keys.append(timeout)

		deferred = self.execute_command("BLPOP", *keys)
		if self.pipelining is False:
			# Success
			return deferred

	def brpop(self, keys, timeout=0):
		"""
		Blocking RPOP
		"""

		# Update last request time
		self._lastreq = time()

		if isinstance(keys, (str, unicode)):
			keys = [keys]
		else:
			keys = list(keys)

		keys.append(timeout)

		deferred = self.execute_command("BRPOP", *keys)
		if self.pipelining is False:
			# Success
			return deferred

	def brpoplpush(self, srckey, dstkey, timeout=0):
		"""
		Return and remove (atomically) the last element of the source
		List  stored at srckey and push the same element to the
		destination List stored at dstkey
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("BRPOPLPUSH", srckey, dstkey, timeout)
		if self.pipelining is False:
			# Success
			return deferred

	def rpoplpush(self, srckey, dstkey):
		"""
		Return and remove (atomically) the last element of the source
		List  stored at srckey and push the same element to the
		destination List stored at dstkey
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("RPOPLPUSH", srckey, dstkey)
		if self.pipelining is False:
			# Success
			return deferred

	def _make_set(self, result):
		if isinstance(result, list):
			return set(result)

		return result

	def sadd(self, key, members):
		"""
		Add the specified member to the Set value at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("SADD", key, *try_to_list(members))
		if self.pipelining is False:
			# Success
			return deferred

	def srem(self, key, members):
		"""
		Remove the specified member from the Set value at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("SREM", key, *try_to_list(members))
		if self.pipelining is False:
			# Success
			return deferred

	def spop(self, key):
		"""
		Remove and return (pop) a random element from the Set value at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("SPOP", key)
		if self.pipelining is False:
			# Success
			return deferred

	def smove(self, srckey, dstkey, member):
		"""
		Move the specified member from one Set to another atomically
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("SMOVE", srckey, dstkey, member)
		if self.pipelining is False:
			# Success
			return deferred

	def scard(self, key):
		"""
		Return the number of elements (the cardinality) of the Set at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("SCARD", key)
		if self.pipelining is False:
			# Success
			return deferred

	def sismember(self, key, value):
		"""
		Test if the specified value is a member of the Set at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("SISMEMBER", key, value)
		if self.pipelining is False:
			# Success
			return deferred

	def sinter(self, keys):
		"""
		Return the intersection between the Sets stored at key1, ..., keyN
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("SINTER", *try_to_list(keys))
		deferred.addCallback(self._make_set)
		if self.pipelining is False:
			# Success
			return deferred

	def sinterstore(self, dstkey, keys):
		"""
		Compute the intersection between the Sets stored
		at key1, key2, ..., keyN, and store the resulting Set at dstkey
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("SINTERSTORE", dstkey, *try_to_list(keys))
		if self.pipelining is False:
			# Success
			return deferred

	def sunion(self, keys):
		"""
		Return the union between the Sets stored at key1, key2, ..., keyN
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("SUNION", *try_to_list(keys))
		deferred.addCallback(self._make_set)
		if self.pipelining is False:
			# Success
			return deferred

	def sunionstore(self, dstkey, keys):
		"""
		Compute the union between the Sets stored
		at key1, key2, ..., keyN, and store the resulting Set at dstkey
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("SUNIONSTORE", dstkey, *try_to_list(keys))
		if self.pipelining is False:
			# Success
			return deferred

	def sdiff(self, keys):
		"""
		Return the difference between the Set stored at key1 and
		all the Sets key2, ..., keyN
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("SDIFF", *try_to_list(keys))
		deferred.addCallback(self._make_set)
		if self.pipelining is False:
			# Success
			return deferred

	def sdiffstore(self, dstkey, keys):
		"""
		Compute the difference between the Set key1 and all the
		Sets key2, ..., keyN, and store the resulting Set at dstkey
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("SDIFFSTORE", dstkey, *try_to_list(keys))
		if self.pipelining is False:
			# Success
			return deferred

	def smembers(self, key):
		"""
		Return all the members of the Set value at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("SMEMBERS", key)
		deferred.addCallback(self._make_set)
		if self.pipelining is False:
			# Success
			return deferred

	def srandmember(self, key, count=None):
		"""
		Return a random member of the Set value at key
		"""

		# Update last request time
		self._lastreq = time()

		if count is None:
			deferred = self.execute_command("SRANDMEMBER", key)
			if self.pipelining is False:
				# Success
				return deferred
		else:
			deferred = self.execute_command("SRANDMEMBER", key, int(count))
			if self.pipelining is False:
				# Success
				return deferred

	def zadd(self, key, score, member, *args):
		"""
		Add the specified member to the Sorted Set value at key
		or update the score if it already exist
		"""

		# Update last request time
		self._lastreq = time()

		if args:
			# Args should be pairs (have even number of elements)
			if len(args) % 2:
				return fail(InvalidData(
					"Invalid number of arguments to ZADD"))
			else:
				l = [score, member]
				l.extend(args)
				args = l
		else:
			args = [score, member]

		deferred = self.execute_command("ZADD", key, *args)
		if self.pipelining is False:
			# Success
			return deferred

	def zrem(self, key, *args):
		"""
		Remove the specified member from the Sorted Set value at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("ZREM", key, *args)
		if self.pipelining is False:
			# Success
			return deferred

	def zincr(self, key, member):
		return self.zincrby(key, 1, member)

	def zdecr(self, key, member):
		return self.zincrby(key, -1, member)

	def zincrby(self, key, increment, member):
		"""
		If the member already exists increment its score by increment,
		otherwise add the member setting increment as score
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("ZINCRBY", key, increment, member)
		if self.pipelining is False:
			# Success
			return deferred

	def zrank(self, key, member):
		"""
		Return the rank (or index) or member in the sorted set at key,
		with scores being ordered from low to high
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("ZRANK", key, member)
		if self.pipelining is False:
			# Success
			return deferred

	def zrevrank(self, key, member):
		"""
		Return the rank (or index) or member in the sorted set at key,
		with scores being ordered from high to low
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("ZREVRANK", key, member)
		if self.pipelining is False:
			# Success
			return deferred

	def _handle_withscores(self, r):
		if isinstance(r, list):
			# Return a list tuples of form (value, score)
			return zip(r[::2], r[1::2])

		return r

	def _zrange(self, key, start, end, withscores, reverse):
		if reverse:
			cmd = "ZREVRANGE"
		else:
			cmd = "ZRANGE"

		if withscores:
			pieces = (cmd, key, start, end, "WITHSCORES")
		else:
			pieces = (cmd, key, start, end)

		r = self.execute_command(*pieces)
		if withscores:
			r.addCallback(self._handle_withscores)

		return r

	def zrange(self, key, start=0, end=-1, withscores=False):
		"""
		Return a range of elements from the sorted set at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self._zrange(key, start, end, withscores, False)
		if self.pipelining is False:
			# Success
			return deferred

	def zrevrange(self, key, start=0, end=-1, withscores=False):
		"""
		Return a range of elements from the sorted set at key,
		exactly like ZRANGE, but the sorted set is ordered in
		traversed in reverse order, from the greatest to the smallest score
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self._zrange(key, start, end, withscores, True)
		if self.pipelining is False:
			# Success
			return deferred

	def _zrangebyscore(self, key, min, max, withscores, offset, count, reverse):
		if reverse:
			cmd = "ZREVRANGEBYSCORE"
		else:
			cmd = "ZRANGEBYSCORE"

		if (offset is None) != (count is None):  # XNOR
			return fail(InvalidData(
				"Invalid count and offset arguments to %s" % cmd))

		if withscores:
			pieces = [cmd, key, min, max, "WITHSCORES"]
		else:
			pieces = [cmd, key, min, max]

		if offset is not None and count is not None:
			pieces.extend(("LIMIT", offset, count))

		r = self.execute_command(*pieces)

		if withscores:
			r.addCallback(self._handle_withscores)

		return r

	def zrangebyscore(self, key, min='-inf', max='+inf', withscores=False, offset=None, count=None):
		"""
		Return all the elements with score >= min and score <= max
		(a range query) from the sorted set
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self._zrangebyscore(key, min, max, withscores, offset, count, False)
		if self.pipelining is False:
			# Success
			return deferred

	def zrevrangebyscore(self, key, max='+inf', min='-inf', withscores=False, offset=None, count=None):
		"""
		ZRANGEBYSCORE in reverse order
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self._zrangebyscore(key, max, min, withscores, offset, count, True)
		if self.pipelining is False:
			# Success
			return deferred

	def zcount(self, key, min='-inf', max='+inf'):
		"""
		Return the number of elements with score >= min and score <= max
		in the sorted set
		"""
		if min == '-inf' and max == '+inf':
			return self.zcard(key)

		# Update last request time
		self._lastreq = time()

		return self.execute_command("ZCOUNT", key, min, max)

	def zcard(self, key):
		"""
		Return the cardinality (number of elements) of the sorted set at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("ZCARD", key)
		if self.pipelining is False:
			# Success
			return deferred

	def zscore(self, key, element):
		"""
		Return the score associated with the specified element of the sorted
		set at key
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("ZSCORE", key, element)
		if self.pipelining is False:
			# Success
			return deferred

	def zremrangebyrank(self, key, min=0, max=-1):
		"""
		Remove all the elements with rank >= min and rank <= max from
		the sorted set
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("ZREMRANGEBYRANK", key, min, max)
		if self.pipelining is False:
			# Success
			return deferred

	def zremrangebyscore(self, key, min, max):
		"""
		Remove all the elements with score >= min and score <= max from
		the sorted set
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("ZREMRANGEBYSCORE", key, min, max)
		if self.pipelining is False:
			# Success
			return deferred

	def zunionstore(self, dstkey, keys, aggregate=None):
		"""
		Perform a union over a number of sorted sets with optional
		weight and aggregate
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self._zaggregate("ZUNIONSTORE", dstkey, keys, aggregate)
		if self.pipelining is False:
			# Success
			return deferred

	def zinterstore(self, dstkey, keys, aggregate=None):
		"""
		Perform an intersection over a number of sorted sets with optional
		weight and aggregate
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self._zaggregate("ZINTERSTORE", dstkey, keys, aggregate)
		if self.pipelining is False:
			# Success
			return deferred

	def _zaggregate(self, command, dstkey, keys, aggregate):
		pieces = [command, dstkey, len(keys)]
		if isinstance(keys, dict):
			keys, weights = zip(*keys.items())
		else:
			weights = None

		pieces.extend(keys)
		if weights:
			pieces.append("WEIGHTS")
			pieces.extend(weights)

		if aggregate:
			if aggregate is min:
				aggregate = 'MIN'
			elif aggregate is max:
				aggregate = 'MAX'
			elif aggregate is sum:
				aggregate = 'SUM'
			else:
				err_flag = True
				if isinstance(aggregate, (str, unicode)):
					aggregate_u = aggregate.upper()
					if aggregate_u in ('MIN', 'MAX', 'SUM'):
						aggregate = aggregate_u
						err_flag = False
				if err_flag:
					return fail(InvalidData(
						"Invalid aggregate function: %s" % aggregate))
			pieces.extend(("AGGREGATE", aggregate))
		return self.execute_command(*pieces)

	def hset(self, key, field, value):
		"""
		Set the hash field to the specified value. Creates the hash if needed
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("HSET", key, field, value)
		if self.pipelining is False:
			# Success
			return deferred

	def hsetnx(self, key, field, value):
		"""
		Set the hash field to the specified value if the field does not exist.
		Creates the hash if needed
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("HSETNX", key, field, value)
		if self.pipelining is False:
			# Success
			return deferred

	def hget(self, key, field):
		"""
		Retrieve the value of the specified hash field.
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("HGET", key, field)
		if self.pipelining is False:
			# Success
			return deferred

	def hmget(self, key, fields):
		"""
		Get the hash values associated to the specified fields.
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("HMGET", key, *fields)
		if self.pipelining is False:
			# Success
			return deferred

	def hmset(self, key, mapping):
		"""
		Set the hash fields to their respective values.
		"""

		# Update last request time
		self._lastreq = time()

		items = []
		for pair in mapping.iteritems():
			items.extend(pair)

		deferred = self.execute_command("HMSET", key, *items)
		if self.pipelining is False:
			# Success
			return deferred

	def hincr(self, key, field):
		return self.hincrby(key, field, 1)

	def hdecr(self, key, field):
		return self.hincrby(key, field, -1)

	def hincrby(self, key, field, integer):
		"""
		Increment the integer value of the hash at key on field with integer.
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("HINCRBY", key, field, integer)
		if self.pipelining is False:
			# Success
			return deferred

	def hexists(self, key, field):
		"""
		Test for existence of a specified field in a hash
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("HEXISTS", key, field)
		if self.pipelining is False:
			# Success
			return deferred

	def hdel(self, key, field):
		"""
		Remove the specified field from a hash
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("HDEL", key, field)
		if self.pipelining is False:
			# Success
			return deferred

	def hlen(self, key):
		"""
		Return the number of items in a hash.
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("HLEN", key)
		if self.pipelining is False:
			# Success
			return deferred

	def hkeys(self, key):
		"""
		Return all the fields in a hash.
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("HKEYS", key)
		if self.pipelining is False:
			# Success
			return deferred

	def hvals(self, key):
		"""
		Return all the values in a hash.
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("HVALS", key)
		if self.pipelining is False:
			# Success
			return deferred

	def hgetall(self, key):
		"""
		Return all the fields and associated values in a hash.
		"""

		# Update last request time
		self._lastreq = time()

		deferred = self.execute_command("HGETALL", key)
		deferred.addCallback(self._hgetall)
		if self.pipelining is False:
			# Success
			return deferred

	def _hgetall(self, result):
		return dict( zip(result[::2], result[1::2]) )

	def pipeline(self):
		# Update last request time
		self._lastreq = time()

		if self.pipelining is True:
			return fail(RedisError("Already pipelining"))

		self.pipelining = True
		self.pipeliningFree = True

		self.pipeliningQueue = []

		# Success
		return succeed(self)

	@inlineCallbacks
	def pipelineExecute(self):
		if self.pipelining is False:
			raise RedisError("Not in pipelining")

		try:
			# Data to send
			sequences = []

			deferreds = []
			responses = []

			for data, response, deferred in self.pipeliningQueue:
				sequences.append(data)

				deferreds.append(deferred)
				responses.append(response)

			if len(sequences) >= 1:
				self.transport.writeSequence(sequences)

			# Wait for responses
			sequences = []

			for i, (status, response) in enumerate((yield DeferredList(responses, consumeErrors=True))):
				callback = deferreds[i].callback if status else deferreds[i].errback
				callback(response)

				sequences.append(deferreds[i])

			# Clean
			del responses
			del deferreds

			# Return responses
			returnValue( (yield DeferredList(sequences, consumeErrors=True)) )
		finally:
			self.pipelining = False
			self.pipeliningQueue = None

			if self.pipeliningFree:
				self.pipeliningFree = False

				if not self.coveredOnClient:
					self.factory.pool.clientFree(self)

	def pipelineDiscard(self):
		if self.pipelining is False:
			return fail(RedisError("Not in pipelining"))

		if self.pipeliningQueue:
			for data, response, deferred in self.pipeliningQueue:
				self.replyQueueLength += 1
				self.replyQueue.put('DISCARD')

		self.pipelining = False
		self.pipeliningQueue = None

		if self.pipeliningFree:
			self.pipeliningFree = False

			if not self.coveredOnClient:
				self.factory.pool.clientFree(self)

		return succeed(True)

	@inlineCallbacks
	def multi(self, keys=None):
		# Update last request time
		self._lastreq = time()

		if self.pipelining is True:
			raise RedisError("Not allow in pipelining")

		if self.transaction is True:
			raise RedisError("Already transaction")

		if keys:
			if isinstance(keys, (str, unicode)):
				keys = [keys]

			response = (yield self.execute_command("WATCH", *keys))
			if response != 'OK':
				raise RedisError('Invalid WATCH response: %s' % response)

		response = (yield self.execute_command("MULTI"))
		if response != 'OK':
			raise RedisError('Invalid MULTI response: %s' % response)

		self.transaction = True
		self.transactionFree = True

		returnValue(self)

	@inlineCallbacks
	def commit(self):
		# Update last request time
		self._lastreq = time()

		if self.pipelining is True:
			raise RedisError("Not allow in pipelining")

		if self.transaction is False:
			raise RedisError("Not in transaction")

		try:
			response = (yield self.execute_command("EXEC"))

			if response is None:
				raise WatchError("Transaction failed")

			self.transaction = False
			self.transactions = 0

			# Try free client
			if self.transactionFree:
				# Mark transaction
				self.transactionFree = False

				if not self.coveredOnClient:
					self.factory.pool.clientFree(self)
		except Exception, exception:
			# Close connection
			self.transport.loseConnection()

			# Trow error
			raise exception
		else:
			returnValue(response)

	@inlineCallbacks
	def discard(self):
		# Update last request time
		self._lastreq = time()

		if self.pipelining is True:
			raise RedisError("Not allow in pipelining")

		if self.transaction is False:
			raise RedisError("Not in transaction")

		try:
			response = (yield self.execute_command("DISCARD"))

			self.transaction = False
			self.transactions = 0

			# Try free client
			if self.transactionFree:
				# Mark transaction
				self.transactionFree = False

				if not self.coveredOnClient:
					self.factory.pool.clientFree(self)
		except Exception, exception:
			# Close connection
			self.transport.loseConnection()

			# Trow error
			raise exception
		else:
			returnValue(response)

	def publish(self, channel, message):
		"""
		Publish message to a channel
		"""

		# Update last request time
		self._lastreq = time()

		if self.pipelining is True:
			return fail(RedisError("Not allow in pipelining"))

		return self.execute_command("PUBLISH", channel, message)

	def save(self):
		"""
		Synchronously save the DB on disk
		"""

		# Update last request time
		self._lastreq = time()

		if self.pipelining is True:
			return fail(RedisError("Not allow in pipelining"))

		return self.execute_command("SAVE")

	def bgsave(self):
		"""
		Asynchronously save the DB on disk
		"""

		# Update last request time
		self._lastreq = time()

		if self.pipelining is True:
			return fail(RedisError("Not allow in pipelining"))

		return self.execute_command("BGSAVE")

	def lastsave(self):
		"""
		Return the UNIX time stamp of the last successfully saving of the
		dataset on disk
		"""

		# Update last request time
		self._lastreq = time()

		if self.pipelining is True:
			return fail(RedisError("Not allow in pipelining"))

		return self.execute_command("LASTSAVE")

	def shutdown(self):
		"""
		Synchronously save the DB on disk, then shutdown the server
		"""

		# Update last request time
		self._lastreq = time()

		if self.pipelining is True:
			return fail(RedisError("Not allow in pipelining"))

		return self.execute_command("SHUTDOWN")

	def bgrewriteaof(self):
		"""
		Rewrite the append only file in background when it gets too big
		"""

		# Update last request time
		self._lastreq = time()

		if self.pipelining is True:
			return fail(RedisError("Not allow in pipelining"))

		return self.execute_command("BGREWRITEAOF")

	def info(self):
		"""
		Provide information and statistics about the server
		"""

		# Update last request time
		self._lastreq = time()

		if self.pipelining is True:
			return fail(RedisError("Not allow in pipelining"))

		return self.execute_command("INFO")

	@inlineCallbacks
	def coverOnClient(self, callable, *a, **kw):
		# Update last request time
		self._lastreq = time()

		if self.coveredOnClient is True:
			raise RedisError("Already covered")

		if self.pipelining is True:
			raise RedisError("Not allow in pipelining")

		if self.transaction is True:
			raise RedisError("Already transaction")

		# Try cover
		self.coveredOnClient = True

		result = None
		resultCover = None

		try:
			result = (yield callable(
				self,
				*a, 
				**kw
			))
		finally:
			self.coveredOnClient = False

		returnValue(result)


class CProtocol(Protocol, Redis):

	def __init__(self, charset="utf-8", errors="strict"):
		Redis.__init__(self, charset, errors)

		self._reader = (hiredis.Reader(
			protocolError=InvalidResponse,
			replyError=ResponseError,
			encoding=self.charset
		))

		self._feed = self._reader.feed
		self._gets = self._reader.gets

	@defer.inlineCallbacks
	def connectionMade(self):
		try:
			if self.factory.password is not None:
				response = (yield self.auth(self.factory.password))
				if isinstance(response, Exception):
					raise response

			if self.factory.db:
				response = yield self.select(self.factory.db)
				if isinstance(response, Exception):
					raise response
		except Exception, e:
			self.factory.continueTrying = True
			self.transport.loseConnection()

			msg = "Redis error: could not set dbid=%s: %s" % \
				  (self.factory.db, str(e))
			#self.factory.connectionError(msg)
			#if self.factory.isLazy:
			#	log.err(msg)
			log.err(ConnectionError(msg))
			defer.returnValue(None)

		self.connected = 1

		if DEBUG:
			log.msg('Redis protocol connection made', self, self.factory)

		self._lastreq = time()
		self.factory.resetDelay()

		if self.factory.deferred is not None:
			self.factory.deferred.callback(self)

			# Clean deferred
			self.factory.deferred = None
		else:
			# pool pendings
			self.factory.pool.delPendings(self.factory)

			# Will is reconnect action
			self.factory.pool.clientFree(self)

	def connectionLost(self, reason):
		if DEBUG:
			log.msg('Redis protocol connection lost', self, self.factory, reason)

		self.cancelCommands(reason)
		self.connected = 0

		# Clean
		self._reader = None

		self._feed = None
		self._gets = None

	def dataReceived(self, data):
		self._feed(data)

		while self.connected:
			result = self._gets()

			if result is False:
				# Stop
				break

			if isinstance(result, Exception):
				self.replyReceived(result)
			elif isinstance(result, (ListType, TupleType)):
				if result is not None:
					result = map(try_to_numeric, result)

				self.replyReceived(result)
			else:
				self.replyReceived(try_to_numeric(result))


class PProtocol(LineReceiver, Redis):
	"""
	Redis client protocol.
	"""

	ERROR = "-"
	STATUS = "+"
	INTEGER = ":"
	BULK = "$"
	MULTI_BULK = "*"

	def __init__(self, charset="utf-8", errors="strict"):
		Redis.__init__(self, charset, errors)

		self.bulk_length = 0
		self.bulk_buffer = ""
		self.multi_bulk_length = 0
		self.multi_bulk_reply = []

	@defer.inlineCallbacks
	def connectionMade(self):
		try:
			if self.factory.password is not None:
				response = (yield self.auth(self.factory.password))
				if isinstance(response, Exception):
					raise response

			if self.factory.db:
				response = yield self.select(self.factory.db)
				if isinstance(response, Exception):
					raise response
		except Exception, e:
			self.factory.continueTrying = True
			self.transport.loseConnection()

			msg = "Redis error: could not set dbid=%s: %s" % \
				  (self.factory.db, str(e))
			#self.factory.connectionError(msg)
			#if self.factory.isLazy:
			#	log.err(msg)
			log.err(ConnectionError(msg))
			defer.returnValue(None)

		self.connected = 1

		if DEBUG:
			log.msg('Redis protocol connection made', self, self.factory)

		self._lastreq = time()
		self.factory.resetDelay()

		if self.factory.deferred is not None:
			self.factory.deferred.callback(self)

			# Clean deferred
			self.factory.deferred = None
		else:
			# pool pendings
			self.factory.pool.delPendings(self.factory)

			# Will is reconnect action
			self.factory.pool.clientFree(self)

	def connectionLost(self, reason):
		if DEBUG:
			log.msg('Redis protocol connection lost', self, self.factory, reason)

		self.cancelCommands(reason)
		self.connected = 0

		LineReceiver.connectionLost(self, reason)

	def lineReceived(self, line):
		"""
		Reply types:
		  "-" error message
		  "+" single line status reply
		  ":" integer number (protocol level only?)
		  "$" bulk data
		  "*" multi-bulk data
		"""
		if not line:
			return

		token = line[0]  # first byte indicates reply type
		data = line[1:]
		if token == self.ERROR:
			self.errorReceived(data)
		elif token == self.STATUS:
			self.statusReceived(data)
		elif token == self.INTEGER:
			self.integerReceived(data)
		elif token == self.BULK:
			try:
				self.bulk_length = long(data)
			except ValueError:
				self.replyReceived(InvalidResponse(
				 "Cannot convert data '%s' to integer" % data))
				return
			if self.bulk_length == -1:
				self.bulkDataReceived(None)
				return
			else:
				self.bulk_length += 2  # \r\n
				self.setRawMode()
		elif token == self.MULTI_BULK:
			try:
				self.multi_bulk_length = long(data)
			except (TypeError, ValueError):
				self.replyReceived(InvalidResponse(
				 "Cannot convert multi-response header '%s' to integer" % \
				 data))
				self.multi_bulk_length = 0
				return
			if self.multi_bulk_length == -1:
				self.multi_bulk_reply = None
				self.multiBulkDataReceived()
				return
			elif self.multi_bulk_length == 0:
				self.multiBulkDataReceived()

	def rawDataReceived(self, data):
		"""
		Process and dispatch to bulkDataReceived.
		"""
		if self.bulk_length:
			data, rest = data[:self.bulk_length], data[self.bulk_length:]
			self.bulk_length -= len(data)
		else:
			rest = ""

		self.bulk_buffer += data
		if self.bulk_length == 0:
			bulk_buffer = self.bulk_buffer[:-2]
			self.bulk_buffer = ""
			self.bulkDataReceived(bulk_buffer)
			while self.multi_bulk_length > 0 and rest:
				if rest[0] == self.BULK:
					idx = rest.find(self.delimiter)
					if idx == -1:
						break
					data_len = int(rest[1:idx], 10)
					if data_len == -1:
						rest = rest[5:]
						self.bulkDataReceived(None)
						continue
					elif len(rest) >= (idx + 5 + data_len):
						data_start = idx + 2
						data_end = data_start + data_len
						data = rest[data_start: data_end]
						rest = rest[data_end + 2:]
						self.bulkDataReceived(data)
						continue
				break
			self.setLineMode(extra=rest)

	def errorReceived(self, data):
		"""
		Error from server.
		"""
		reply = ResponseError(data[4:] if data[:4] == "ERR " else data)

		if self.multi_bulk_length:
			self.handleMultiBulkElement(reply)
		else:
			self.replyReceived(reply)

	def statusReceived(self, data):
		"""
		Single line status should always be a string.
		"""
		#if data == "none":
		#	reply = None # should this happen here in the client?
		#else:
		#	reply = data

		reply = data
		if reply == "QUEUED":
			self.transactions += 1
			self.replyReceived(reply)
			return

		if self.multi_bulk_length:
			self.handleMultiBulkElement(reply)
		else:
			self.replyReceived(reply)

	def integerReceived(self, data):
		"""
		For handling integer replies.
		"""
		try:
			reply = int(data)
		except ValueError:
			reply = InvalidResponse(
			"Cannot convert data '%s' to integer" % data)

		if self.multi_bulk_length:
			self.handleMultiBulkElement(reply)
		else:
			self.replyReceived(reply)

	def bulkDataReceived(self, data):
		"""
		Receipt of a bulk data element.
		"""
		if data is None:
			element = data
		else:
			try:
				element = int(data) if data.find('.') == -1 else float(data)
			except (ValueError):
				try:
					element = data.decode(self.charset)
				except UnicodeDecodeError:
					element = data
		if self.multi_bulk_length > 0:
			self.handleMultiBulkElement(element)
		else:
			self.replyReceived(element)

	def handleMultiBulkElement(self, element):
		if self.transaction:
			self.transactions -= 1
		self.multi_bulk_reply.append(element)
		self.multi_bulk_length = self.multi_bulk_length - 1

		if self.multi_bulk_length == 0:
			self.multiBulkDataReceived()

	def multiBulkDataReceived(self):
		"""
		Receipt of list or set of bulk data elements.
		"""
		if self.transaction and self.transactions == 0:
			self.transaction = False

		reply = self.multi_bulk_reply
		self.multi_bulk_reply = []
		self.multi_bulk_length = 0
		self.replyReceived(reply)


class RedisFactory(ReconnectingClientFactory):

	noisy = DEBUG

	maxRetries = None
	maxDelay = 3

	factor = 1.7182818284590451

	if hiredis is None:
		protocol = PProtocol
	else:
		protocol = CProtocol

	protocolInstance = None
	protocolTimeout = 10

	def __init__(self):
		self.deferred = defer.Deferred()

	def startedConnecting(self, connector):
		self.connector = connector

	def clientConnectionLost(self, connector, reason):
		"""
		Notify the pool that we've lost our connection.
		"""

		if self.protocolInstance is not None:
			if (self.pool.countOpenedConnections > 1 
					or self.protocolInstance._lastreq and ((time() - self.protocolInstance._lastreq) >= self.protocolTimeout)):

				self.stopTrying()

		if self.continueTrying:
			ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

			# pool pendings
			self.pool.addPendings(self)
		else:
			if DEBUG:
				log.msg('Redis factory lost', self)

			if self.deferred:
				self.deferred.errback(reason)

				# Clean deferred
				self.deferred = None
			else:
				# pool pendings
				self.pool.delPendings(self)

		if self.protocolInstance is not None:
			self.pool.clientGone(self.protocolInstance)

		# Clean
		self.protocolInstance = None

	def clientConnectionFailed(self, connector, reason):
		"""
		Notify the pool that we're unable to connect
		"""

		if self.protocolInstance is not None:
			if (self.pool.countOpenedConnections > 1 
					or self.protocolInstance._lastreq and ((time() - self.protocolInstance._lastreq) >= self.protocolTimeout)):

				self.stopTrying()

		if self.continueTrying:
			ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)

			# pool pendings
			self.pool.addPendings(self)
		else:
			if DEBUG:
				log.msg('Redis factory failed', self)

			if self.deferred:
				self.deferred.errback(reason)

				# Clean deferred
				self.deferred = None
			else:
				# pool pendings
				self.pool.delPendings(self)

		if self.protocolInstance is not None:
			self.pool.clientGone(self.protocolInstance)

		# Clean
		self.protocolInstance = None

	def buildProtocol(self, addr):
		"""
		Attach the C{self.pool} to the protocol so it can tell it, when we've connected.
		"""
		if self.protocolInstance is not None:
			self.pool.clientGone(self.protocolInstance)

		self.protocolInstance = self.protocol()
		self.protocolInstance.factory = self

		self.resetDelay()

		return self.protocolInstance

	def __str__(self):
		return '<RedisFactory %02d instance at 0x%X>' % (self.counter, id(self))


class RedisConnectionPool(object):

	def __init__(self, address, db=0, poolsize=1, password=None, **params):
		assert isinstance(address,
			(UNIXAddress, IPv4Address)), 'Address type [%s] not supported!' % type(address)

		self._address = address
		self._counter = 0
		self._counterCommands = 0
		self._counterCommandsAll = 0

		self._busyClients = []
		self._freeClients = []

		self._pendingsList = []

		self._commands = []
		self._poolsize = poolsize
		self._partials = dict()

		self._db = db
		self._password = password or None
		self._params = params

	def closeConnections(self, force=False):
		if force:
			f = list(self._pendingsList)
			c = list()

			c.extend(self._freeClients)
			c.extend(self._busyClients)

			self._busyClients = []
			self._freeClients = []

			self._pendingsList = []

			for factory in f:
				factory.stopTrying()
				factory.continueTrying = 0

			for client in c:
				client.factory.stopTrying()
				client.factory.continueTrying = 0
				client.transport.loseConnection()

	@property
	def countOpenedConnections(self):
		return (len(self._busyClients) + len(self._freeClients) + len(self._pendingsList))

	def addPendings(self, factory):
		if DEBUG:
			log.msg('Redis pool addPendings', factory)

		if not factory in self._pendingsList:
			# Add factory to pendings list
			self._pendingsList.append(factory)

	def delPendings(self, factory):
		if DEBUG:
			log.msg('Redis pool delPendings', factory)

		if factory in self._pendingsList:
			# Delete factory from pendings list
			self._pendingsList.remove(factory)

	def clientConnect(self):
		"""
		Create a new client connection.

		@return: A L{Deferred} that fires with the L{IProtocol} instance.
		"""
		self._counter += 1

		factory = RedisFactory()
		factory.pool = self
		factory.counter = self._counter
		factory.db = self._db
		factory.password = self._password

		if 'dict' in self._params:
			factory.dictConstructor = self._params['dict']

		self.addPendings(factory)

		if isinstance(self._address, IPv4Address):
			(reactor.connectTCP(
				self._address.host,
				self._address.port,
				factory=factory,
				timeout=5
			))
		elif isinstance(self._address, UNIXAddress):
			(reactor.connectUNIX(
				self._address.name,
				factory=factory,
				timeout=5
			))
		else:
			raise RuntimeError('Redis unknown address type!')

		def _cb(client):
			self.delPendings(factory)

			if client is not None:
				self.clientFree(client)

		def _eb(reason):
			if reason:
				log.err(reason)

			self.delPendings(factory)

		deferred = factory.deferred
		deferred.addCallbacks(_cb, _eb)

		# Wait for new connection
		return deferred

	def pendingCommand(self, method, args, kwargs):
		if DEBUG:
			log.msg('Redis pool pending', method, args, kwargs)

		deferred = defer.Deferred()

		if deferred:
			self._commands.append((deferred, method, args, kwargs))

		# Wait in this deferred
		return deferred

	def performRequestOnClient(self, client, method, args, kwargs):
		if DEBUG:
			log.msg('Redis pool use connection', client, client.factory)

		self._counterCommands += 1
		self._counterCommandsAll += 1

		self.clientBusy(client)

		# Request
		return (getattr(client, method)(*args, **kwargs)
			.addBoth(self._bbPerformRequestOnClient, client=client))

		# Fail, method not exists
		self.clientFree(client)

		return defer.fail(AttributeError(client, method))

	def _bbPerformRequestOnClient(self, result, client):
		self.clientFree(client)

		# Got result on next callback
		return result

	def performRequest(self, method, *args, **kwargs):
		"""
		Select an available client and perform the given request on it.

		@parma method: The method to call on the client.

		@parma args: Any positional arguments that should be passed to C{command}.
		@param kwargs: Any keyword arguments that should be passed to C{command}.

		@return: A L{Deferred} that fires with the result of the given command.
		"""
		if len(self._freeClients):
			return self.performRequestOnClient(self._freeClients.pop(), method, args, kwargs)

		if not (len(self._busyClients) + len(self._pendingsList)) >= self._poolsize:
			self.clientConnect()

		# Wait for request
		return self.pendingCommand(method, args, kwargs)

	def clientGone(self, client):
		"""
		Notify that the given client is to be removed from the pool completely.

		@param client: An instance of a L{Protocol}.
		"""
		if DEBUG:
			log.msg('Redis pool gone connection', client, client.factory)

		if client in self._busyClients:
			self._busyClients.remove(client)

		if client in self._freeClients:
			self._freeClients.remove(client)

	def clientBusy(self, client):
		"""
		Notify that the given client is being used to complete a request.

		@param client: An instance of a L{Protocol}.
		"""
		if DEBUG:
			log.msg('Redis pool busy connection', client, client.factory)

		if client in self._freeClients:
			self._freeClients.remove(client)

		self._busyClients.append(client)

	def clientFree(self, client):
		"""
		Notify that the given client is free to handle more requests.

		@param client: An instance of a L{Protocol}.
		"""
		if client.transaction:
			if DEBUG:
				log.msg('Redis pool free in transaction connection', client, client.factory)
		elif client.pipelining:
			if DEBUG:
				log.msg('Redis pool free in pipelining connection', client, client.factory)
		else:
			if DEBUG:
				log.msg('Redis pool free connection', client, client.factory)

			if client in self._busyClients:
				self._busyClients.remove(client)

			if self._commands:
				(deferred, method, args, kwargs) = self._commands.pop(0)

				# With chain deferred
				(self.performRequestOnClient(
					client,
					method,
					args,
					kwargs
				).chainDeferred(deferred))
			else:
				# Free client
				self._freeClients.append(client)

	def __getattr__(self, name):
		try:
			# Return method if exists
			return self._partials[name]
		except KeyError:
			self._partials[name] = (partial(
				self.performRequest, name
			))

			# Success
			return self._partials[name]

