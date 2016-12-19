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

#include <fstream>
#include <string>
#include <deque>
#include <maxscale/thread.h>

#include "datapoint.hh"

using std::string;

/** Data relay class that relays the data to a destination */
class Relay
{
public:
    Relay(const char *uri);
    virtual ~Relay();

    /**
     * @brief Append data to the relay
     *
     * The data gets added to the internal queue if there is space left for it.
     * If the queue doesn't have enough space, the call will block until enough
     * space is freed.
     *
     * @param d Datapoint to enqueue
     */
    void enqueue(const Datapoint& d);

    /**
     * @brief Send one datapoint string
     *
     * All subclasses that intend to change the transmission method should
     * override this function.
     *
     * @param data Data to send in JSON format
     * @return True if sending of the data was successful, false if an error occurred.
     */
    virtual bool send(const std::string& data) = 0;

private:
    std::deque<std::string> queue; /**< The data queue */
    size_t max_queue_size; /**< Maximum queue size in kibibytes */

    static void process_queue(void *data); /**< Data processing function */
    volatile bool is_running; /**< Whether the thread should continue processing */
    THREAD process_thr; /**< Queue processing thread */
};
