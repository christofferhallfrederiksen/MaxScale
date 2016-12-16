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

#include <maxscale/cppdefs.hh>
#include <jansson.h>
#include <set>
#include <string>
#include <maxscale/buffer.h>
#include <maxscale/session.h>
#include <maxscale/query_classifier.h>

class Datapoint
{
public:
    Datapoint(SESSION *ses, GWBUF* buf);
    Datapoint(Datapoint&& d);
    ~Datapoint();

    /**
     * Return JSON representation of this datapoint
     *
     * @return JSON representation of this datapoint
     */
    json_t* toJSON() const;

    const std::string& toString()const;

    bool operator ==(const Datapoint& a) const;

private:
    qc_query_op_t op;
    uint32_t type;
    std::string to_string;
    json_t *json;

    Datapoint(const Datapoint&) = delete;
    Datapoint operator=(const Datapoint&) = delete;
};

/** Hashing function for Datapoint */
namespace std
{
template<> struct hash<Datapoint>
{
    typedef Datapoint argument_type;
    typedef std::size_t result_type;
    result_type operator()(argument_type const& s) const
    {
        return std::hash<std::string> {}(s.toString());
    }
};

template <> struct equal_to<Datapoint>
{
    typedef Datapoint first_argument_type;
    typedef Datapoint second_argument_type;
    typedef bool result_type;

    result_type operator()(const first_argument_type& a, const second_argument_type& b) const
    {
        return a == b;
    }
};
}
