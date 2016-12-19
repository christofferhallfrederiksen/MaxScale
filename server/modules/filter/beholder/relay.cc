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

#include <stdexcept>

#include "datapoint.hh"
#include "file_relay.hh"

#define DEFAULT_MAX_QUEUE_SIZE 1024

void Relay::process_queue(void *data)
{
    Relay* self = reinterpret_cast<Relay*>(data);

    while (self->is_running)
    {
        if (self->queue.size() > 0)
        {
            if (!self->send(*self->queue.cbegin()))
            {
                MXS_ERROR("Failed to send data.");
            }
            else
            {
                /** Data was successfully sent, remove it from the queue */
                self->queue.pop_front();
            }
        }
        else
        {
            /** No data to process, wait for more */
            thread_millisleep(1);
        }
    }
}

Relay::Relay(const char *uri):
    max_queue_size(DEFAULT_MAX_QUEUE_SIZE), is_running(true)
{
    thread_start(&this->process_thr, Relay::process_queue, this);
}

Relay::~Relay()
{
    pthread_join(this->process_thr, NULL);
}

void Relay::enqueue(const Datapoint& d)
{
    this->queue.push_back(d.toString());

    while (this->queue.size() > this->max_queue_size)
    {
        /** Queue is full, wait until it clears up */
        thread_millisleep(1);
    }
}
