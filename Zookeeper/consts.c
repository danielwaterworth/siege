#include "consts.h"

struct Id *const_anyone_id_unsafe() { return &ZOO_ANYONE_ID_UNSAFE; }

struct Id *const_auth_ids() { return &ZOO_AUTH_IDS; }

struct ACL_vector *const_open_acl_unsafe() { return &ZOO_OPEN_ACL_UNSAFE; }

struct ACL_vector *const_read_acl_unsafe() { return &ZOO_READ_ACL_UNSAFE; }

struct ACL_vector *const_creator_all_acl() { return &ZOO_CREATOR_ALL_ACL; }
