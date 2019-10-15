import frappe

import asyncio
import signal

import aioredis, pickle, zlib
from aiokafka import AIOKafkaConsumer

from six import string_types
from sys import exit
from latte.utils.background.job import Task, get_kafka_conf

loop = asyncio.get_event_loop()

GRACEFUL_SHUTDOWN_WAIT = 10	

def start(queue, quiet, worker_type=None):
	'''
	This method starts the gevent background worker instance.
	:param queue: name of the queue to fetch jobs from
	:param quiet: 
	:param worker_type: name of worker type (redis(default) or kafka)
	'''
	worker_type = worker_type or 'redis'
	print(f'Starting Gevent worker on queue {queue} of {worker_type}')
	loop.add_signal_handler(signal.SIGINT, graceful_shutdown)
	loop.add_signal_handler(signal.SIGTERM, graceful_shutdown)
	loop.run_until_complete(deque_and_enqueue(queue, worker_type))

async def deque_and_enqueue(queue, worker_type):
	'''
	This method dequeues the enqueued jobs in the queue
	:param queue: name of the queue/topic 
	:param worker_type: name of worker type (redis(default) or kafka)
	'''
	if worker_type != 'kafka':
		async for task in fetch_jobs_from_redis(queue, loop):
			task.process_task()
	else:
		topic = f'kafka_{queue}'
		async for task in fetch_jobs_from_kafka(topic, loop):
			task.process_task()


def graceful_shutdown():
	'''
	This method shuts down the gevent background workers
	'''
	print('Warm shutdown requested')
	graceful = Task.pool.join(timeout=GRACEFUL_SHUTDOWN_WAIT)
	print('Shutting down, Gracefully=', graceful)
	exit(0 if graceful else 1)

async def fetch_jobs_from_redis(queue, loop):
	'''
	This async method fetches job from redisQ, yeilds a Task Object with Job meta-data
	
	:param queue: name of the queue to fetch jobs from
	:param loop: event loop object
	'''
	try:
		print('Connecting')
		conn = await aioredis.create_connection('redis://localhost:11000', loop=loop)
		print('Connected')
		while True:
			#poping job from redis's 'queue'
			job_id = str((await conn.execute('blpop', f'rq:queue:{queue}', 0))[1], 'utf-8')
			job_meta = await conn.execute('hgetall', f'rq:job:{job_id}')
			job_dict = frappe._dict()
			for idx, data in enumerate(job_meta):
				try:
					job_meta[idx] = str(data, 'utf-8')
				except UnicodeDecodeError:
					job_meta[idx] = data
			for i in range(0, len(job_meta), 2):
				job_dict[job_meta[i]] = job_meta[i + 1]

			_, _, _, job_kwargs = pickle.loads(zlib.decompress(job_dict.data))

			yield Task(**job_kwargs) 
			await conn.execute('del', f'rq:job:{job_id}')
	finally:
		conn.close()
		await conn.wait_closed()

async def fetch_jobs_from_kafka(topic, loop):
		'''
		This async method fetches job from kafkaQ, yeilds a Task object
		:param topic: topic to which the jobs are produced
		:param loop: event loop object
		'''
		print('Connecting to Kafka..')

		with frappe.init_site():
			conf = get_kafka_conf()

		consumer = AIOKafkaConsumer(
			topic, loop = loop,
			bootstrap_servers = conf.get('bootstrap.servers'),
			group_id = conf.get('group.id'))
		await consumer.start()
		try:
			print('KafkaQ Connected..')
			async for job in consumer:
				task = pickle.loads(zlib.decompress(job.value))
				yield task
		finally:
			await consumer.stop()