# Endianess for storing numbers
ENDIAN = 'little'

# Bytes used for storing keys or values
KEY_BYTES = 16
VALUE_BYTES = 16

# Bytes used for storing references to pages
# Can address 16 TB of memory with 4 KB pages
PAGE_REFERENCE_BYTES = 4

# Bytes used for storing the type of the node in page header
NODE_TYPE_BYTES = 1

# Bytes used for storing the length of the page payload in page header
USED_PAGE_LENGTH_BYTES = 3

# Bytes used for storing the length of the key or value payload in record
# header. Limits the maximum length of a key or value to 64 KB.
USED_KEY_LENGTH_BYTES = 2
USED_VALUE_LENGTH_BYTES = 2

# Bytes used for storing general purpose integers like file metadata
OTHERS_BYTES = 4
