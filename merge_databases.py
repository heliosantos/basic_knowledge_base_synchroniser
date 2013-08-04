import pymongo

import database_connections

databases = database_connections.get_databases()
crud_logs = []

# merge all crud logs
for database in databases:
  for crud_log in database.crud_loggers.find():
    crud_logs.append({'database': database, 'crud_log': crud_log})
  
  
  
crud_logs = sorted(crud_logs, key=lambda crud_log: crud_log['crud_log']['updated_at'], reverse=True)



while len(crud_logs) > 0:

  crud_log = crud_logs.pop(0)
  database = crud_log['database']
  crud_log = crud_log['crud_log']
  
  for db in databases:
    if db == database:
      continue
    
    op = crud_log['operation']
    if op == 'created' or op == 'updated':
      article = database.articles.find_one({'permalink': crud_log['permalink']}, {'_id': 0})
      
      if article == None:
        continue
      print '-----------'
      print(db['name'])
      print crud_log['permalink']
      print article
      db.articles.update({'permalink': crud_log['permalink']}, {"$set": article}, upsert=True)
      
      
      
    elif op == 'deleted':
      db.articles.remove({'permalink': crud_log['permalink']})

    db.crud_loggers.remove({'permalink': crud_log['permalink']})  
  database.crud_loggers.remove({'permalink': crud_log['permalink']})  
      
      
      