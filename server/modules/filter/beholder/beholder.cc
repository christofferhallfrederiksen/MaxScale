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
#include <maxscale/cppdefs.hh>
#include <memory>
#include <sstream>
#include <unordered_map>
#include <time.h>
#include <string>
#include <maxscale/filter.hh>
#include <maxscale/modinfo.h>
#include <maxscale/alloc.h>
#include <maxscale/modutil.h>
#include <maxscale/modulecmd.h>
#include <maxscale/spinlock.hh>

#include "datapoint.hh"
#include "file_relay.hh"
#include "redis_relay.hh"

/**
 * @file
 *
 * Filter for calculating and reporting query characteristics
 */

class Beholder;

class BeholderSession : public mxs::FilterSession
{
public:

    BeholderSession(MXS_SESSION *session, Beholder *parent) :
        mxs::FilterSession(session),
        instance(parent),
        session(session) { }

    int routeQuery(GWBUF *queue);

    MXS_SESSION* getSession() const
    {
        return session;
    }

private:
    Beholder *instance;
    MXS_SESSION* session;
};

class Beholder : public mxs::Filter<Beholder, BeholderSession>
{
public:
    static Beholder* create(const char* name, char** options, MXS_CONFIG_PARAMETER* parameters);
    BeholderSession* newSession(MXS_SESSION* session);
    void diagnostics(DCB* dcb);

    static int64_t getCapabilities()
    {
        return RCAP_TYPE_CONTIGUOUS_INPUT;
    }

    // Non-API functions
    void process_datapoint(BeholderSession *ses, GWBUF *queue);
    void clear_data();
    std::string toString();

private:
    /** Constructors */
    Beholder(const char* name, char** options, MXS_CONFIG_PARAMETER* parameters);
    Beholder operator=(const Beholder& b);

    std::unordered_map<Datapoint, int> datapoints;
    time_t instance_started;
    time_t latest_group_added;
    int stabilization_period;
    std::unique_ptr<Relay> relay;
    SPINLOCK lock;
};

MXS_BEGIN_DECLS

bool beholder_show_data(const MODULECMD_ARG *args)
{
    MXS_FILTER *filter = filter_def_get_instance(args->argv[1].value.filter);
    Beholder *beholder = reinterpret_cast<Beholder*> (filter);
    DCB *dcb = args->argv[0].value.dcb;

    std::string str = beholder->toString();
    dcb_printf(dcb, "%s\n", str.c_str());

    return true;
}

bool beholder_clear_data(const MODULECMD_ARG *args)
{
    MXS_FILTER *filter = filter_def_get_instance(args->argv[1].value.filter);
    Beholder *beholder = reinterpret_cast<Beholder*> (filter);
    beholder->clear_data();
    return true;
}

/**
 * The module entry point routine. It is this routine that must populate
 * the structure that is referred to as the "module object", this is a
 * structure with the set of external entry points for this module.
 *
 * @return The module object
 */
MXS_MODULE* MXS_CREATE_MODULE()
{
    modulecmd_arg_type_t args[] =
    {
        { MODULECMD_ARG_OUTPUT, "DCB for output"},
        { MODULECMD_ARG_FILTER, "Show data for this filter" }
    };
    modulecmd_register_command("beholder", "data", beholder_show_data, 2, args);

    modulecmd_arg_type_t reset_args[] =
    {
        { MODULECMD_ARG_FILTER, "Clear data for this filter" }
    };
    modulecmd_register_command("beholder", "data/clear", beholder_clear_data, 1, reset_args);

    static MXS_MODULE info =
    {
        MXS_MODULE_API_FILTER,
        MXS_MODULE_ALPHA_RELEASE,
        MXS_FILTER_VERSION,
        "Data relay filter",
        "V1.0.0",
        &Beholder::s_object,
        NULL, /* Process init. */
        NULL, /* Process finish. */
        NULL, /* Thread init. */
        NULL, /* Thread finish. */
        {
            {"uri", MXS_MODULE_PARAM_STRING},
            {MXS_END_MODULE_PARAMS}
        }
    };

    return &info;
}

MXS_END_DECLS

static Relay* create_new_relay(const char *uri)
{
    if (strstr(uri, "file://") == uri)
    {
        uri += 7;
        return new FileRelay(uri);
    }
    else if (strstr(uri, "redis://") == uri)
    {
        uri += 8;
        return new RedisRelay(uri);
    }
    else
    {
        string err("Invalid relay URI: ");
        err += uri;
        throw std::runtime_error(err.c_str());
    }
}

Beholder::Beholder(const char* name, char** options, MXS_CONFIG_PARAMETER* params):
    instance_started(time(0)),
    latest_group_added(time(0)),
    stabilization_period(300),
    relay(std::unique_ptr<Relay>(create_new_relay(config_get_string(params, "uri"))))

{
}

Beholder* Beholder::create(const char* name, char** options, MXS_CONFIG_PARAMETER* parameters)
{
    Beholder *inst = NULL;

    MXS_EXCEPTION_GUARD(inst = new Beholder(name, options, parameters));

    return inst;
}

BeholderSession* Beholder::newSession(MXS_SESSION* session)
{
    BeholderSession *ses = NULL;
    MXS_EXCEPTION_GUARD(ses = new BeholderSession(session, this));
    return ses;
}

void Beholder::diagnostics(DCB* dcb)
{
    for (const auto& i : this->datapoints)
    {
        dcb_printf(dcb, "%s: %d\n", i.first.toString().c_str(), i.second);
    }
}

int BeholderSession::routeQuery(GWBUF *queue)
{
    if (modutil_is_SQL(queue))
    {
        MXS_EXCEPTION_GUARD(this->instance->process_datapoint(this, queue));
    }

    return mxs::FilterSession::routeQuery(queue);
}

void Beholder::process_datapoint(BeholderSession *ses, GWBUF *queue)
{
    Datapoint p(ses->getSession(), queue);
    std::unordered_map<Datapoint, int>::iterator iter = this->datapoints.find(p);

    this->relay->enqueue(p);

    if (iter != this->datapoints.end())
    {
        iter->second++;
    }
    else
    {
        this->datapoints[std::move(p)] =  1;
        this->latest_group_added = time(NULL);

        if (this->latest_group_added - this->instance_started > this->stabilization_period)
        {
            char *sql = modutil_get_SQL(queue);
            DCB *dcb = ses->getSession()->client_dcb;
            MXS_WARNING("Unexpected query behavior from '%s@%s': %s",
                        dcb->user, dcb->remote, sql ? sql : "(SQL extraction failed)");
            MXS_FREE(sql);
        }
    }
}

std::string Beholder::toString()
{
    json_t *arr = json_array();
    mxs::SpinLockGuard(this->lock);

    for (auto& a : this->datapoints)
    {
        json_array_append_new(arr, a.first.toJSON());
    }

    char *json = json_dumps(arr, JSON_PRESERVE_ORDER);
    ss_dassert(json);
    std::string s(json);
    MXS_FREE(json);
    return s;
}

void Beholder::clear_data()
{
    this->datapoints.clear();
    time_t now = time(NULL);
    this->instance_started = now;
    this->latest_group_added = now;
}
