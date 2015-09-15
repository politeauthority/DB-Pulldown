databases = {}

databases['master']               = {}
databases['master']['info']       = 'DB MASTER'
databases['master']['host']       = 'ourfancysite.com'
databases['master']['user']       = 'admin'
databases['master']['pass']       = 'fancypassword'
databases['master']['port']       = '3306' 					# DEFAULT PORT WILL BE USED
databases['master']['engine']     = 'mysql'

databases['tools']                = {}
databases['tools']['info']        = 'Data Tools Database'
databases['tools']['host']        = '192.168.1.100'
databases['tools']['user']        = 'admin'
databases['tools']['pass']        = 'fancypassword'
databases['tools']['engine']      = 'mysql'

databases['dev']                  = {}
databases['dev']['info']          = 'Company Development Database'
databases['dev']['host']          = '192.168.1.150'
databases['dev']['user']          = 'dbadmin'
databases['dev']['pass']          = 'fancypassword'
databases['dev']['engine']        = 'mysql'

#database aliases 
databases['extra']                = databases['tools']
databases['db1']                  = databases['master']
databases['dbmaster']             = databases['master']
