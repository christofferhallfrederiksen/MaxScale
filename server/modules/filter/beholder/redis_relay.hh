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
#pragma once

#include "relay.hh"
#include <hiredis/hiredis.h>

using std::ofstream;
using std::endl;

/** Data relay class that relays the data to a destination */
class RedisRelay: public Relay
{
public:
    RedisRelay(const char *uri);
    ~RedisRelay();

    /**
     * @brief Send one datapoint string
     *
     * This writes the JSON string to the Redis server
     *
     * @param data Data to send in JSON format
     * @return True if sending of the data was successful, false if an error occurred.
     */
    bool send(const std::string& data);

private:
    redisContext *context;
    std::string list_name;
};
