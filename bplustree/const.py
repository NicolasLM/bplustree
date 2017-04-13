# Endianess for storing numbers
ENDIAN = 'little'

# Bytes used for storing keys
KEY_BYTES = 8

# Bytes used for storing values
VALUE_BYTES = 8

# Bytes used for storing references to pages
# Can address 16 TB of memory with 4 KB pages
PAGE_REFERENCE_BYTES = 4

# Bytes used for storing the type of the node in page header
NODE_TYPE_BYTES = 1

# Bytes used for storing the length of the page payload in page header
USED_PAGE_LENGTH_BYTES = 3

# Bytes used for storing general purpose integers like file metadata
OTHERS_BYTES = 4
