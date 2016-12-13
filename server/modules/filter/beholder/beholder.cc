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
#include <list>
#include <memory>
#include <string>
#include <sstream>
#include <unordered_map>
#include <set>
#include <time.h>
#include <jansson.h>
#include <maxscale/filter.hh>
#include <maxscale/modinfo.h>
#include <maxscale/query_classifier.h>
#include <maxscale/alloc.h>
#include <maxscale/modutil.h>

/**
 * @file
 *
 * Filter for calculating and reporting query characteristics
 */

MODULE_INFO info =
{
    MODULE_API_FILTER,
    MODULE_ALPHA_RELEASE,
    FILTER_VERSION,
    "Query characteristics filter"
};

class Datapoint
{
public:
    Datapoint(SESSION *ses, GWBUF* buf);
    ~Datapoint();

    /**
     * Return string representation of this datapoint
     *
     * @return String representation of this datapoint
     */
    const std::string& toString() const;

    bool operator ==(const Datapoint& a) const;

private:
    qc_query_op_t op;
    uint32_t type;
    size_t n_fields;
    int select;
    int subselect;
    int set;
    int where;
    int group;
    std::set<std::string> tables;
    std::set<std::string> columns;
    std::set<std::string> databases;
    std::string to_string;
    json_t *json;
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

class Beholder;

class BeholderSession : public mxs::FilterSession
{
public:

    BeholderSession(SESSION *session, Beholder *parent) :
        mxs::FilterSession(session),
        instance(parent),
        session(session) { }

    int routeQuery(GWBUF *queue);

    SESSION* getSession() const
    {
        return session;
    }

private:
    Beholder *instance;
    SESSION* session;
};

class Beholder : public mxs::Filter<Beholder, BeholderSession>
{
public:
    static Beholder* create(const char* name, char** options, FILTER_PARAMETER** parameters);
    BeholderSession* newSession(SESSION* session);
    void diagnostics(DCB* dcb);

    static int64_t getCapabilities()
    {
        return RCAP_TYPE_CONTIGUOUS_INPUT;
    }

    // Non-API functions
    void process_datapoint(BeholderSession *ses, GWBUF *queue);

private:
    std::unordered_map<Datapoint, int> datapoints;
    time_t instance_started;
    time_t latest_group_added;
    int stabilization_period;
    SPINLOCK lock;
};

MXS_BEGIN_DECLS

static char version_str[] = "V1.0.0";

/**
 * Implementation of the mandatory version entry point
 *
 * @return version string of the module
 */
char* version()
{
    return version_str;
}

/**
 * The module initialization routine, called when the module
 * is first loaded.
 */
void ModuleInit()
{
}

/**
 * The module entry point routine. It is this routine that
 * must populate the structure that is referred to as the
 * "module object", this is a structure with the set of
 * external entry points for this module.
 *
 * @return The module object
 */
FILTER_OBJECT* GetModuleObject()
{
    return &Beholder::s_object;
}

MXS_END_DECLS

Beholder* Beholder::create(const char* name, char** options, FILTER_PARAMETER** parameters)
{
    Beholder *inst = NULL;

    MXS_EXCEPTION_GUARD(inst = new Beholder);

    if (inst)
    {
        time_t now = time(NULL);
        inst->instance_started = now;
        inst->latest_group_added = now;
        inst->stabilization_period = 300;
        spinlock_init(&inst->lock);
    }

    return inst;
}

BeholderSession* Beholder::newSession(SESSION* session)
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

    if (iter != this->datapoints.end())
    {
        this->datapoints[p]++;
    }
    else
    {
        this->datapoints[p] =  1;
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

Datapoint::Datapoint(SESSION *ses, GWBUF* buf):
    op(qc_get_operation(buf)), type(qc_get_type(buf)), select(0), subselect(0),
    set(0), where(0), group(0), to_string("")
{
    const QC_FIELD_INFO *infos = NULL;
    this->n_fields = 0;
    qc_get_field_info(buf, &infos, &this->n_fields);

    json_t *obj = json_object();

    json_t *s_user = json_string(ses->client_dcb->user);
    json_object_set_new(obj, "user", s_user);

    json_t *s_addr = json_string(ses->client_dcb->remote);
    json_object_set_new(obj, "address", s_addr);

    char *tm = qc_typemask_to_string(this->type);
    json_t *s_tm = json_string(tm);
    json_object_set_new(obj, "type", s_tm);
    MXS_FREE(tm);

    const char *qc_op = qc_op_to_string(this->op);
    json_t *s_qc_op = json_string(qc_op);
    json_object_set_new(obj, "op", s_qc_op);

    char *sql = modutil_get_SQL(buf);
    json_t *s_sql = json_string(sql);
    json_object_set_new(obj, "sql", s_sql);
    MXS_FREE(sql);

    char *canonical = modutil_get_canonical(buf);
    json_t *s_canonical = json_string(canonical);
    json_object_set_new(obj, "canonical_sql", s_canonical);
    MXS_FREE(canonical);

    json_t *arr = json_array();

    for (size_t i = 0; i < this->n_fields; i++)
    {
        json_t *item = json_object();
        json_t *col = json_string(infos[i].column);
        json_object_set(obj, "column", col);

        if (infos[i].table)
        {
            json_t *tab = json_string(infos[i].table);
            json_object_set(obj, "table", tab);
        }

        if (infos[i].database)
        {
            json_t *db = json_string(infos[i].database);
            json_object_set(obj, "db", db);
        }

        json_t *used_in = json_array();

        if (infos[i].usage & QC_USED_IN_SELECT)
        {
            json_t *usage = json_string("select");
            json_array_append(used_in, usage);
        }
        if (infos[i].usage & QC_USED_IN_SUBSELECT)
        {
            json_t *usage = json_string("subselect");
            json_array_append(used_in, usage);
        }
        if (infos[i].usage & QC_USED_IN_WHERE)
        {
            json_t *usage = json_string("where");
            json_array_append(used_in, usage);
        }
        if (infos[i].usage & QC_USED_IN_SET)
        {
            json_t *usage = json_string("set");
            json_array_append(used_in, usage);
        }
        if (infos[i].usage & QC_USED_IN_GROUP_BY)
        {
            json_t *usage = json_string("group_by");
            json_array_append(used_in, usage);
        }

        json_object_set_new(item, "usage", used_in);
        json_array_append(arr, item);
    }

    json_object_set_new(obj, "fields", arr);
    char *json_str = json_dumps(obj, JSON_PRESERVE_ORDER);
    this->to_string = std::string(json_str);
    this->json = obj;
    MXS_FREE(json_str);
}

Datapoint::~Datapoint()
{
    json_decref(this->json);
}

const std::string& Datapoint::toString() const
{
    return this->to_string;
}

bool Datapoint::operator ==(const Datapoint& a) const
{
    return this->op == a.op &&
           this->type == a.type &&
           this->n_fields == a.n_fields &&
           this->select == a.select &&
           this->set == a.set &&
           this->where == a.where &&
           this->group == a.group;
}
