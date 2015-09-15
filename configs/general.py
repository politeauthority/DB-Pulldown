general = {}

# Default source database
general['default_source_db']         = 'db3'

# Default Destination database
general['default_dest_db']           = 'alix'

# Location for downloads
general['pulldown_location']         = '/home/alix/backups'

# Databases which the tool is not allowed to write to
general['no_write_databases']        = [ 'master', 'db2', 'db3', 'db5', 'officeslave', 'slave' ]

# Backup dir can get this big in gigs before deletion
general['alloted_backup_size']       = 75     


# Notifications
general['use_push_bullet']           = True
general['use_push_bullet_key']       = 'alix'