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
#include "file_relay.hh"

FileRelay::FileRelay(const char *uri):
    Relay(uri)
{
    this->destination = string(uri);
    size_t opts = this->destination.find("?");

    if (opts != string::npos)
    {
        this->options = this->destination.substr(opts + 1);
        this->destination[opts] = '\0';
    }

    this->output = ofstream(this->destination);
}

FileRelay::~FileRelay()
{
    this->output.close();
}

bool FileRelay::send(const std::string& data)
{
    this->output << data << endl;
    return true;
}
