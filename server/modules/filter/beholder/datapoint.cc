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
#include <maxscale/alloc.h>
#include <maxscale/modutil.h>

Datapoint::Datapoint(SESSION *ses, GWBUF* buf):
    op(qc_get_operation(buf)), type(qc_get_type(buf))
{
    const QC_FIELD_INFO *infos = NULL;
    size_t n_fields = 0;
    qc_get_field_info(buf, &infos, &n_fields);

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

    char *canonical = modutil_get_canonical(buf);
    json_t *s_canonical = json_string(canonical);
    json_object_set_new(obj, "canonical_sql", s_canonical);
    MXS_FREE(canonical);

    json_t *arr = json_array();

    for (size_t i = 0; i < n_fields; i++)
    {
        /** Object for this field */
        json_t *item = json_object();

        /** Columns in this field */
        json_t *col = json_string(infos[i].column);
        json_object_set_new(item, "column", col);

        if (infos[i].table)
        {
            json_t *tab = json_string(infos[i].table);
            json_object_set_new(item, "table", tab);
        }

        if (infos[i].database)
        {
            json_t *db = json_string(infos[i].database);
            json_object_set_new(item, "db", db);
        }

        /** How this field is used */
        json_t *used_in = json_array();

        if (infos[i].usage & QC_USED_IN_SELECT)
        {
            json_t *usage = json_string("select");
            json_array_append_new(used_in, usage);
        }
        if (infos[i].usage & QC_USED_IN_SUBSELECT)
        {
            json_t *usage = json_string("subselect");
            json_array_append_new(used_in, usage);
        }
        if (infos[i].usage & QC_USED_IN_WHERE)
        {
            json_t *usage = json_string("where");
            json_array_append_new(used_in, usage);
        }
        if (infos[i].usage & QC_USED_IN_SET)
        {
            json_t *usage = json_string("set");
            json_array_append_new(used_in, usage);
        }
        if (infos[i].usage & QC_USED_IN_GROUP_BY)
        {
            json_t *usage = json_string("group_by");
            json_array_append_new(used_in, usage);
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

Datapoint::Datapoint(Datapoint&& d)
{
    this->op = d.op;
    this->type = d.type;
    this->to_string = d.to_string;
    this->json = d.json;
    d.json = NULL;
}

Datapoint::~Datapoint()
{
    if (this->json)
    {
        json_decref(this->json);
    }
}

json_t* Datapoint::toJSON() const
{
    return json_incref(this->json);
}

const std::string& Datapoint::toString()const
{
    return to_string;
}

bool Datapoint::operator==(const Datapoint& a) const
{
    return json_equal(this->json, a.json) == 1;
}
