#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdarg.h>


#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ev.h>
#include <openssl/ssl.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_iterator.h>
#include <aerospike/as_status.h>
#include <aerospike/as_query.h>
#include <aerospike/as_ldt.h>
#include <aerospike/as_list.h>
#include <aerospike/as_tls.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_iterator.h>
#include <aerospike/as_arraylist_iterator.h>
#include <aerospike/aerospike_lstack.h>
#include <aerospike/aerospike_query.h>


#define AS_OK               0
#define AS_NEED_RECONNECT   1
#define AS_SHM_ERR          2
#define AS_SHM_CREATE_ERR   3
#define AS_SHM_TRUNCATE_ERR 4
#define AS_CLIENT_ERROR     5

#define LSTACK_PEEK         100


/**
 * Open persistent connection to Aerospike
 *
 * @var const char * hostName
 * @var const int port
 * @var const shar * shaMemId
 */
static int private_connect_as(const char *hostName, const int port, const char* shaMemId) {
    /* ini shared memc */
    int shm = 0;

    /* read pointer from shared memc */
    if ( (shm = shm_open(shaMemId, O_CREAT|O_RDWR, 0777)) == -1 ) {
        return AS_SHM_CREATE_ERR;
    }

    /* set size */
    if ( ftruncate(shm, sizeof(aerospike)) == -1 ) {
        return AS_SHM_TRUNCATE_ERR;
    }

    /* get client pointer */
    aerospike * as = (aerospike *)mmap(0, sizeof(aerospike), PROT_WRITE|PROT_READ, MAP_SHARED, shm, 0);

    /* errors */
    as_error err;

    /* init as_client */
    as_config config;
    as_config_init(&config);
    as_config_add_host(&config, hostName, port);

    aerospike_init(as, &config);

    /* connection */
    aerospike_connect(as, &err);

    /* close shared memc */
    munmap(as, sizeof(aerospike));
    close(shm);

    as_error_reset(&err);

    if(err.code>0)
    {
        return AS_CLIENT_ERROR;
    }
    return 1;
}


/**
 * Open persistent connection to Aerospike
 *
 * @var lua_State * L
 */
static int disconnect_as(lua_State *L) {

    /* shared memc ID */
    const char* shaMemId = luaL_checkstring(L, 1);

    int shm = 0;

    if ( (shm = shm_open(shaMemId, O_RDWR, 0777)) == -1 ) {
        lua_pushnumber(L, AS_SHM_ERR);
        lua_pushstring(L, "AS_SHM_ERR");
        return 2;
    }

    /* get client pointer */
    aerospike * as = (aerospike *)mmap(0, sizeof(aerospike), PROT_WRITE|PROT_READ, MAP_SHARED, shm, 0);

    as_error err;

    /* close connection and destroy obj */
    aerospike_close(as, &err);
    aerospike_destroy(as);

    /* return to LUA result message */
    lua_pushnumber(L, err.code);
    lua_pushstring(L, err.message);

    /* cleaning */
    munmap(as, sizeof(aerospike));
    close(shm);
    shm_unlink(shaMemId);

    lua_pushnumber(L, AS_OK);
    lua_pushstring(L, "AS_OK");
    return 2;
}


/**
 * Query result struct
 */
typedef struct {
       //стек
       lua_State * L;
       int main_i;

} query_struct;

/**
 * Write bin to result struct
 *
 * @var const char * name
 * @var const as_val * value
 * @var void * tmpbuf
 */
bool print_bin2buf(const char * name, const as_val * value, void * tmpbuf) {

    /* get struct and set type */
    query_struct * this_tmp_buf = (query_struct *)tmpbuf;

    /* is number ? */
    as_integer * ivalue = as_integer_fromval(value);
    char * svalue = as_val_tostring(value);

    /* second array key */
    lua_pushstring(this_tmp_buf->L, name);

    /* write, num/str */
    if (ivalue) {
        lua_pushnumber(this_tmp_buf->L, as_integer_get(ivalue));
    } else {
       lua_pushstring(this_tmp_buf->L, svalue);
    }
    /* close key */
    lua_settable(this_tmp_buf->L, -3);

    /* cleaning */
    as_integer_destroy(ivalue);
    free(svalue);

    return true;
}

/**
 * Callback query method
 *
 * @var const as_val *value
 * @var void * tmpbuf
 */
bool query_proc(const as_val *value, void * tmpbuf) {
    if (value == NULL) {
        /* query is made */
        return true;
    }

    as_record *rec = as_record_fromval(value);

    if (rec != NULL) {
        /* get struct and set type */
        query_struct * this_tmp_buf = (query_struct *)tmpbuf;

        /* index */
        lua_pushnumber(this_tmp_buf->L,this_tmp_buf->main_i);
        this_tmp_buf->main_i++;

        /* second array */
        lua_newtable(this_tmp_buf->L);

        /* fill result of query */
        as_record_foreach(rec, print_bin2buf, tmpbuf);

        /* close key */
        lua_settable(this_tmp_buf->L,-3);
    }

    /* cleaning */
    as_record_destroy(rec);

    return true;
}

/**
 * Query layer
 *
 * @var lua_State *L
 */
static int query_as(lua_State *L){

    as_error err;

    const int new_execution = lua_tointeger(L, 1); /* execute new connection */
    const char* shaMemId = luaL_checkstring(L, 2); /* id in Shared Memory */
    const char *hostName = luaL_checkstring(L, 3); /* db connection hostname */
    const int port = lua_tointeger(L, 4); /* db connection port */
    const char* nameSpace = luaL_checkstring(L, 5); /* db namespace */
    const char* set = luaL_checkstring(L, 6); /* db set */
    const char* wherebin = luaL_checkstring(L, 7); /* binname (secondkey) */
    const char* wheredata = luaL_checkstring(L, 8); /* selection condition */
    const uint32_t query_timeout = lua_tointeger(L, 9); /* query timeout, ms */
    const int get_pk = lua_tointeger(L, 10); /* execute via primary key */

    /* if just run create connection */
    if(new_execution==1)
    {
        const int connect_status = private_connect_as(hostName, port, shaMemId);
        if(connect_status!=1)
        {
            lua_pushnil(L);
            lua_pushnumber(L, connect_status);
            lua_pushstring(L, "AS_CONNECT_ERR");
            return 3;
        }
    }

    int shm = 0;

    /* open shm segment */
    if ( (shm = shm_open(shaMemId, O_RDWR, 0777)) == -1 ) {
        lua_pushnil(L);
        lua_pushnumber(L, AS_SHM_ERR);
        lua_pushstring(L, "AS_SHM_ERR");
        return 3;
    }

    /* set pointer on aerospike obj */
    aerospike * as = (aerospike *)mmap(0, sizeof(aerospike), PROT_WRITE|PROT_READ, MAP_SHARED, shm, 0);


    /* create structure and fill it */
    query_struct tmp_buf;
    tmp_buf.L = L;
    tmp_buf.main_i = 0;

    /* create LUA table */
    lua_newtable(L);


    /* primary key query ? */
    if(get_pk == 1)
    {
        as_key key;
        as_key_init(&key, nameSpace, set, wheredata);
        as_record * rec = NULL;
        if ( aerospike_key_get(as, &err, NULL, &key, &rec) != AEROSPIKE_OK ) {

            /* cleaning */
            as_record_destroy(rec);
            as_key_destroy(&key);

            munmap(as, sizeof(aerospike));
            close(shm);

            lua_pushnil(L);
            lua_pushnumber(L, AS_CLIENT_ERROR);
            lua_pushstring(L, "AS_CLIENT_ERROR");
            return 3;
        }
        else
        {
            /* index */
            lua_pushnumber(tmp_buf.L, 0);

            /* secondary array */
            lua_newtable(tmp_buf.L);

            /* fill result */
            as_record_foreach(rec, print_bin2buf, &tmp_buf);

            /* close key */
            lua_settable(tmp_buf.L,-3);

            /* cleaning */
            as_record_destroy(rec);
            as_key_destroy(&key);
        }
    }
    /* query via secondary key */
    else
    {
        as_query query;
        as_query_init(&query, nameSpace, set);

        as_query_where_init(&query, 1);
        as_query_where(&query, wherebin, as_string_equals(wheredata));

        /* query config, timeout */
        as_policy_query policy;
        as_policy_query_init(&policy);

        if(query_timeout)
        {
            policy.timeout = query_timeout;
        }

        if (aerospike_query_foreach(as, &err, &policy, &query, query_proc, &tmp_buf) !=
               AEROSPIKE_OK) {

            /* cleaning */
            as_query_destroy(&query);

            munmap(as, sizeof(aerospike));
            close(shm);
            lua_pushnil(L);
            lua_pushnumber(L, AS_NEED_RECONNECT);
            lua_pushstring(L, "AS_NEED_RECONNECT");
            return 3;
        }

        /* cleaning */
        as_query_destroy(&query);
    }

    //lua_pushlightuserdata(L, as);

    munmap(as, sizeof(aerospike));
    close(shm);


    lua_pushnumber(L, AS_OK);
    lua_pushstring(L, "AS_OK");
    return 3;
}

/**
 * Query lstack layer
 *
 * @var lua_State *L
 */
static int lstack_as(lua_State *L){

    as_error err;

    const int new_execution = lua_tointeger(L, 1); /* execute new connection */
    const char* shaMemId = luaL_checkstring(L, 2); /* id in Shared Memory */
    const char *hostName = luaL_checkstring(L, 3); /* db connection hostname */
    const int port = lua_tointeger(L, 4); /* db connection port */
    const char* nameSpace = luaL_checkstring(L, 5); /* db namespace */
    const char* set = luaL_checkstring(L, 6); /* db set */
    const char* pk = luaL_checkstring(L, 7); /* primary key */
    const char* stackname = luaL_checkstring(L, 8); /* stack name (binname) */
    const int operation = lua_tointeger(L, 9); /* operation */
    const uint32_t stack_count = lua_tointeger(L, 10); /* stack size */
    //const uint32_t query_timeout = lua_tointeger(L, 11); /* timeout , ms */


    /* if just run create connection */
    if(new_execution==1)
    {
        const int connect_status = private_connect_as(hostName, port, shaMemId);
        if(connect_status!=1)
        {
            lua_pushnil(L);
            lua_pushnumber(L, connect_status);
            lua_pushstring(L, "AS_CONNECT_ERR");
            return 3;
        }
    }


    int shm = 0;

    /* open shm segment */
    if ( (shm = shm_open(shaMemId, O_RDWR, 0777)) == -1 ) {
        lua_pushnil(L);
        lua_pushnumber(L, AS_SHM_ERR);
        lua_pushstring(L, "AS_SHM_ERR");
        return 3;
    }

    /* set pointer on aerospike obj */
    aerospike * as = (aerospike *)mmap(0, sizeof(aerospike), PROT_WRITE|PROT_READ, MAP_SHARED, shm, 0);


    as_key key;
    as_key_init(&key, nameSpace, set, pk);
    as_ldt stack;
    as_ldt_init(&stack, stackname, AS_LDT_LSTACK, NULL);

    if(operation==LSTACK_PEEK)
    {
        uint32_t peek_count = stack_count;
	as_list* p_list = NULL;

      	if (aerospike_lstack_peek(as, &err, NULL, &key, &stack, peek_count,
			&p_list) != AEROSPIKE_OK)
        {
            /* cleaning */
            as_list_destroy(p_list);
            as_key_destroy(&key);
            as_ldt_destroy(&stack);

            munmap(as, sizeof(aerospike));
            close(shm);
            lua_pushnil(L);
            lua_pushnumber(L, AS_CLIENT_ERROR);
            lua_pushstring(L, "AS_CLIENT_ERROR");
            return 3;
        }
        else {
            /* process result */

            int array_i = 0;

            /* create table */
            lua_newtable(L);

            /* init stack iterator */
            as_arraylist_iterator it;
            as_arraylist_iterator_init(&it, (const as_arraylist*)p_list);

            while (as_arraylist_iterator_has_next(&it)) {
                    const as_val * p_val = as_arraylist_iterator_next(&it);
                    char * p_str = as_val_tostring(p_val);

                    /* number ? */
                    as_integer * ivalue = as_integer_fromval(p_val);

                    array_i++;
                    /* write, num/str */
                    if (ivalue) {
                        lua_pushnumber(L, as_integer_get(ivalue));
                    } else {
                        lua_pushstring(L, p_str);
                    }

                    /* close var */
                    lua_rawseti (L, -2, array_i);

                    /* cleaning */
                    as_integer_destroy(ivalue);
                    as_val_destroy(p_val);
                    free(p_str);
            }

            /* destroy struct */
            as_arraylist_iterator_destroy(&it);
            as_list_destroy(p_list);
        }
    }

    /* cleaning */
    as_key_destroy(&key);
    as_ldt_destroy(&stack);

    /* unmap shm */
    munmap(as, sizeof(aerospike));
    close(shm);

    /* return result */
    lua_pushnumber(L, AS_OK);
    lua_pushstring(L, "AS_OK");
    return 3;
}


/**
 * Bind methods struct
 */
static const struct luaL_Reg as_client [] = {
		{"query_as", query_as},
		{"disconnect_as", disconnect_as},
		{"lstack_as", lstack_as},
		{NULL, NULL}
};

/**
 * Execute binding
 */
extern int luaopen_xLuaAerospikeLayer(lua_State *L){
	luaL_register(L, "xLuaAerospikeLayer", as_client);
	return 0;
}
