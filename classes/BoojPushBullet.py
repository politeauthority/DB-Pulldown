#!/usr/bin/python
"""
  PushBullet
  Simplified class to handle sending messages to the Booj Data dept.
  @why: Yes, there are many repos which handle this type of functionalilty
    however here we wrap the basic needs of booj in a simple standard callable
    stack for booj developers.
"""
import sys
import requests
import json
import getopt

class BoojPushBullet( object ):

  def __init__( self ):
    self.api_url      = 'https://api.pushbullet.com/v2/pushes'
    self._json_header = {'Content-Type': 'application/json'}    
    self.users        = {
      'data' : {
        'alix'  : 'v1GrJQUYFFV0NsuokSs4IBplk4EIuzDiZBujCWnq8Rli8',
        'brian' : '',
        'chris' : '',
        'evan'  : '',
        'xan'   : ''
      }
    }
    self.headers = {
      'User-Agent': "%s/%s +%s" % (
        'Booj Tools',
        '1.0',
        'http://www.booj.com/' )
    }

  def send( self, user, title, message ):
    """
      @params :
        user    : str() or list()
        title   : str()
        message : str()
      @return : None
    """
    users = self.__handle_users( user )
    for user in users:
      api_key = user
      payload = {
        'type' : 'note',
        'title': title,
        'body' : message
      }
      payload = json.dumps( payload )
      session = requests.Session()
      session.auth = ( api_key, "" )
      session.headers.update( self._json_header )
      r = session.post( self.api_url, data=payload )
      if r:
        return True
      else:
        return False

  def __handle_users( self, users ):
    api_keys = []
    if isinstance( users, list ):
      for user in users:
        if user in self.users['data']:
          api_keys.append( self.users['data'][user] )
        elif len( user ) == 45:
          api_keys.append( user )
        else:
          print 'Error: Could not find PushBullet API key for ', user
    else:
      if users == 'all' or users == 'data' or users == None:
        for user, key in self.users['data']:
          if key != '' or None:
            api_key.append( key )
      if users in self.users['data']:
        api_keys.append( self.users['data'][users] )
    return api_keys

def __opt_search( key_search, opts, default = None ):
  """
    Search through the options return the arg if available or True if its set
  """
  if not isinstance( key_search, list ):
    key_search = [ key_search ]
  for opt, arg in opts:
    if opt in key_search:
      if not arg:
        return True
      return arg
  if default:
    return default
  return False

def usage( ):
  print ' -- Booj Push Bullet -- '
  print ''
  print 'Parameters'
  print '  --user, -u'
  print '  --title, -t'
  print '  --message, -m'

if __name__ == '__main__':
  """
    Executes if the script from the command line
  """
  user_args = sys.argv[1:]
  if len( user_args ) < 2:
    usage()
    sys.exit()

  try:
    opts, args = getopt.getopt( user_args, "hg:v", 
      ["help",
      "user=",
      "u=",
      "title=",
      "t",
      "message=", 
      "m="
      ] )
  except getopt.GetoptError, e:
    print 'Error: Problem parsing arguments'
    print e
    usage()
    sys.exit()
  users   = __opt_search( ['--user', '-u'], opts )
  title   = __opt_search( ['--title', '-t'], opts )
  message = __opt_search( ['--message', '-m'], opts )
  BoojPushBullet().send( users, title, message )

# End File: BoojPushBullet.py
