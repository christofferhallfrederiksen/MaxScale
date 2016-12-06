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
#include <maxscale/cdefs.h>
#include <list>
#include <memory>
#include <string>
#include <sstream>
#include <unordered_map>
#include <set>
#include <time.h>
#include <maxscale/filter.h>
#include <maxscale/modinfo.h>
#include <maxscale/query_classifier.h>
#include <maxscale/alloc.h>
#include <maxscale/modutil.h>

/**
 * @file
 *
 * Filter for calculating and reporting query characteristics
 */

#define CPP_GUARD(statement)\
    do { try { statement; }                                              \
    catch (const std::exception& x) { MXS_ERROR("Caught standard exception: %s", x.what()); }\
    catch (...) { MXS_ERROR("Caught unknown exception."); } } while (false)

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
    Datapoint(GWBUF*);
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

typedef struct
{
    std::unordered_map<Datapoint, int> datapoints;
    time_t instance_started;
    time_t latest_group_added;
    int    stabilization_period;
    SPINLOCK lock;
} BHD_INSTANCE;

typedef struct
{
    DCB* dcb; /**< Client DCB, used to send error messages */
    DOWNSTREAM down;
} BHD_SESSION;

void process_datapoint(BHD_INSTANCE *inst, BHD_SESSION *ses, GWBUF *queue);

MXS_BEGIN_DECLS

static char version_str[] = "V1.0.0";

static FILTER *createInstance(const char *name, char **options, FILTER_PARAMETER **params);
static void *newSession(FILTER *instance, SESSION *session);
static void closeSession(FILTER *instance, void *session);
static void freeSession(FILTER *instance, void *session);
static void setDownstream(FILTER *instance, void *fsession, DOWNSTREAM *downstream);
static int routeQuery(FILTER *instance, void *fsession, GWBUF *queue);
static void diagnostic(FILTER *instance, void *fsession, DCB *dcb);
static uint64_t getCapabilities(void);

static FILTER_OBJECT MyObject =
{
    createInstance,
    newSession,
    closeSession,
    freeSession,
    setDownstream,
    NULL, // No upstream requirement
    routeQuery,
    NULL, // No clientReply
    diagnostic,
    getCapabilities,
    NULL, // No destroyInstance
};

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
    return &MyObject;
}

/**
 * Create an instance of the filter for a particular service
 * within MaxScale.
 *
 * @param name      The name of the instance (as defined in the config file).
 * @param options   The options for this filter
 * @param params    The array of name/value pair parameters for the filter
 *
 * @return The instance data for this new instance
 */
static FILTER* createInstance(const char *name, char **options, FILTER_PARAMETER **params)
{
    BHD_INSTANCE *inst = NULL;
    CPP_GUARD(inst = new BHD_INSTANCE);

    if (inst)
    {
        time_t now = time(NULL);
        inst->instance_started = now;
        inst->latest_group_added = now;
        inst->stabilization_period = 300;
        spinlock_init(&inst->lock);
    }

    return (FILTER*)inst;
}

/**
 * Associate a new session with this instance of the filter.
 *
 * @param instance  The filter instance data
 * @param session   The session itself
 * @return Session specific data for this session
 */
static void* newSession(FILTER *instance, SESSION *session)
{
    BHD_SESSION *ses = NULL;
    CPP_GUARD(ses = new BHD_SESSION);

    if (ses)
    {
        ses->dcb = session->client_dcb;
    }

    return ses;
}

/**
 * Close a session with the filter, this is the mechanism
 * by which a filter may cleanup data structure etc.
 *
 * The gatekeeper flushes the hashtable to disk every time a session is closed.
 *
 * @param instance  The filter instance data
 * @param session   The session being closed
 */
static void closeSession(FILTER *instance, void *session)
{
}

/**
 * Free the memory associated with this filter session.
 *
 * @param instance  The filter instance data
 * @param session   The session being closed
 */
static void freeSession(FILTER *instance, void *session)
{
    BHD_SESSION *ses = reinterpret_cast<BHD_SESSION*>(session);
    delete ses;
}

/**
 * Set the downstream component for this filter.
 *
 * @param instance  The filter instance data
 * @param session   The session being closed
 * @param downstream    The downstream filter or router
 */
static void setDownstream(FILTER *instance, void *session, DOWNSTREAM *downstream)
{
    BHD_SESSION *ses = reinterpret_cast<BHD_SESSION*>(session);
    ses->down = *downstream;
}

/**
 * @brief Main routing function
 *
 * @param instance  The filter instance data
 * @param session   The filter session
 * @param queue     The query data
 * @return 1 on success, 0 on error
 */
static int routeQuery(FILTER *instance, void *session, GWBUF *queue)
{
    BHD_INSTANCE *inst = reinterpret_cast<BHD_INSTANCE*>(instance);
    BHD_SESSION *ses = reinterpret_cast<BHD_SESSION*>(session);

    if (modutil_is_SQL(queue))
    {
        CPP_GUARD(process_datapoint(inst, ses, queue));
    }

    return ses->down.routeQuery(ses->down.instance, ses->down.session, queue);
}

/**
 * @brief Diagnostics routine
 *
 * @param   instance    The filter instance
 * @param   fsession    Filter session
 * @param   dcb         The DCB for output
 */
static void diagnostic(FILTER *instance, void *fsession, DCB *dcb)
{
    BHD_INSTANCE *inst = reinterpret_cast<BHD_INSTANCE*>(instance);

    for (const auto& i : inst->datapoints)
    {
        dcb_printf(dcb, "%s: %d\n", i.first.toString().c_str(), i.second);
    }
}

/**
 * @brief Capability routine.
 *
 * @return The capabilities of the filter.
 */
static uint64_t getCapabilities(void)
{
    return RCAP_TYPE_STMT_INPUT;
}

MXS_END_DECLS

/**
 * Internal code
 */

void process_datapoint(BHD_INSTANCE *inst, BHD_SESSION *ses, GWBUF *queue)
{
    Datapoint p(queue);
    std::unordered_map<Datapoint, int>::iterator iter = inst->datapoints.find(p);

    if (iter != inst->datapoints.end())
    {
        inst->datapoints[p]++;
    }
    else
    {
        inst->datapoints[p] =  1;
        inst->latest_group_added = time(NULL);

        if (inst->latest_group_added - inst->instance_started > inst->stabilization_period)
        {
            char *sql = modutil_get_SQL(queue);
            MXS_WARNING("Unexpected query behavior from '%s@%s': %s",
                        ses->dcb->user, ses->dcb->remote, sql ? sql : "(SQL extraction failed)");
            MXS_FREE(sql);
        }
    }
}

Datapoint::Datapoint(GWBUF* buf):
    op(qc_get_operation(buf)), type(qc_get_type(buf)), select(0), subselect(0),
    set(0), where(0), group(0), to_string("")
{
    const QC_FIELD_INFO *infos = NULL;
    this->n_fields = 0;
    qc_get_field_info(buf, &infos, &this->n_fields);

    for (size_t i = 0; i < this->n_fields; i++)
    {
        this->columns.insert(std::string(infos[i].column));

        if (infos[i].table)
        {
            this->tables.insert(std::string(infos[i].table));
        }

        if (infos[i].database)
        {
            this->databases.insert(std::string(infos[i].database));
        }

        if (infos[i].usage & QC_USED_IN_SELECT)
        {
            this->select++;
        }
        else if (infos[i].usage & QC_USED_IN_SUBSELECT)
        {
            this->subselect++;
        }
        else if (infos[i].usage & QC_USED_IN_WHERE)
        {
            this->where++;
        }
        else if (infos[i].usage & QC_USED_IN_SET)
        {
            this->set++;
        }
        else if (infos[i].usage & QC_USED_IN_GROUP_BY)
        {
            this->group++;
        }
    }

    char *tm = qc_typemask_to_string(this->type);
    const char *qc_op = qc_op_to_string(this->op);

    std::stringstream ss;

    ss << tm << qc_op << this->n_fields << this->select << this->subselect
       << this->set << this->where << this->group;

    for (const auto& a : this->columns)
    {
        ss << a;
    }

    for (const auto& a : this->tables)
    {
        ss << a;
    }

    for (const auto& a : this->databases)
    {
        ss << a;
    }

    this->to_string = ss.str();
    MXS_FREE(tm);
}

Datapoint::~Datapoint()
{
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
