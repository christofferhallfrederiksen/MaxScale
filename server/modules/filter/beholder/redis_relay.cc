/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl.
 *
 * Change Date: 2019-07-01
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#include "datapoint.hh"
#include "redis_relay.hh"

RedisRelay::RedisRelay(const char *uri) :
    Relay(uri), list_name("beholder_redis_list")
{
    char addr[strlen(uri) + 1];
    strcpy(addr, uri);

    char *opts = strchr(addr, '?');

    if (opts)
    {
        *opts++ = '\0';

        if (strstr(opts, "list=") == opts)
        {
            opts += 5;
            this->list_name = std::string(opts);
        }
        else
        {
            std::string err("Unknown URI option: ");
            err += opts;
            throw std::runtime_error(err);
        }
    }

    char *port_str = strchr(addr, ':');
    int port = 6379;

    if (port_str)
    {
        *port_str++ = '\0';
        port = atoi(port_str);
    }

    if (!(this->context = redisConnect(addr, port)) || this->context->err)
    {
        std::string err("Could not connect to: ");
        err += uri;
        throw std::runtime_error(err);
    }
}

RedisRelay::~RedisRelay()
{
    redisFree(this->context);
}

bool RedisRelay::send(const std::string& data)
{
    bool rval = false;
    redisReply *reply = (redisReply*)redisCommand(this->context, "LPUSH %s %s", this->list_name.c_str(),
                                                  data.c_str());

    if (reply)
    {
        switch (reply->type)
        {
            case REDIS_REPLY_INTEGER:
                rval = true;
                break;

            case REDIS_REPLY_ERROR:
            case REDIS_REPLY_STRING:
            case REDIS_REPLY_STATUS:
                MXS_ERROR("Redis server replied with a message: %.*s", (int)reply->len, reply->str);
                break;

            case REDIS_REPLY_NIL:
                MXS_ERROR("Redis server replied with a nil object.");
                break;

            case REDIS_REPLY_ARRAY:
                MXS_ERROR("Redis server replied with an array.");
                break;

            default:
                ss_dassert(false);
                break;
        }

        freeReplyObject(reply);
    }

    return rval;
}
