from datetime import datetime, timedelta
now = datetime.now()
before = now - timedelta(days=200)
now = '%04d-%02d-%02d' % (now.year, now.month, now.day)
before = '%04d-%02d-%02d' % (before.year, before.month, before.day)

