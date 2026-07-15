/*****************************************************************************\
 * Copyright 2023 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

extern "C" {
#if HAVE_CONFIG_H
#include "config.h"
#endif
}

#include <iostream>
#include <string>

#include "jobspec.hpp"
#include "constraint.hpp"
#include "hostlist_constraint.hpp"
#include "rank_constraint.hpp"

using namespace Flux::Jobspec;

// Derived Constraint classes:
//
class PropertyConstraint : public Constraint {
   public:
    PropertyConstraint (const YAML::Node &);
    PropertyConstraint () = default;
    ~PropertyConstraint () = default;

   private:
    //  Original RFC 31 tokens, kept verbatim so as_yaml() round-trips exactly.
    std::vector<std::string> values;

    //  Pre-interned query terms. Interning the query once here means match()
    //  does no string allocation or hashing at all -- each term is checked
    //  against the vertex's interned property map in O(1).
    struct term {
        Flux::resource_model::property_istr_t key;
        Flux::resource_model::property_istr_t value;
        bool has_value = false;  //!< token was "name=value" rather than "name"
        bool negate = false;     //!< token was "^name"
    };
    std::vector<term> m_terms;

   public:
    virtual bool match (const Flux::resource_model::resource_t &resource) const;
    virtual YAML::Node as_yaml () const;
};

class ConditionalConstraint : public Constraint {
   public:
    ConditionalConstraint (std::string &, const YAML::Node &);
    ConditionalConstraint () = default;
    ~ConditionalConstraint () {};

   private:
    std::string op;
    std::vector<std::unique_ptr<Constraint>> values;

   public:
    virtual bool match (const Flux::resource_model::resource_t &resource) const;
    bool match_and (const Flux::resource_model::resource_t &resource) const;
    bool match_or (const Flux::resource_model::resource_t &resource) const;

    virtual YAML::Node as_yaml () const;
};

std::unique_ptr<Constraint> Flux::Jobspec::constraint_parser (const YAML::Node &constraint)
{
    YAML::const_iterator it;
    YAML::Node operands;

    if (!constraint.IsMap ())
        throw parse_error (constraint, "constraint is not a mapping");
    if (constraint.size () > 1)
        throw parse_error (constraint, "constraint map may not contain > 1 operation");
    if (constraint.size () == 0)
        return std::unique_ptr<Constraint> (new Constraint ());

    it = constraint.begin ();
    std::string operation = it->first.as<std::string> ();
    operands = it->second;

    if (!operands.IsSequence ()) {
        std::string msg = operation + " operator value must be an array";
        throw parse_error (operands, msg.c_str ());
    }

    if (operation == "properties")
        return std::unique_ptr<PropertyConstraint> (new PropertyConstraint (operands));
    else if (operation == "hostlist")
        return std::unique_ptr<HostlistConstraint> (new HostlistConstraint (operands));
    else if (operation == "ranks")
        return std::unique_ptr<RankConstraint> (new RankConstraint (operands));
    else if (operation == "and" || operation == "or" || operation == "not")
        return std::unique_ptr<ConditionalConstraint> (
            new ConditionalConstraint (operation, operands));

    std::string msg = "unknown constraint operator: " + operation;
    throw parse_error (constraint, msg.c_str ());
}

// base Constraint implementation
//

// Base Constraint returns an empty map
YAML::Node Constraint::as_yaml () const
{
    return YAML::Node (YAML::NodeType::Map);
}

// Base Constraint always matches
bool Constraint::match (const Flux::resource_model::resource_t &r) const
{
    return true;
}

// PropertyConstraint implementation
//

PropertyConstraint::PropertyConstraint (const YAML::Node &properties)
{
    for (auto &&property : properties) {
        // YAML::Node::as<std:string> will work on a non-string YAML node.
        //  use Tag() instead to determine if the scalar is a quoted string,
        //   in which case "!" is returned.
        //
        //  Ref: https://yaml.org/spec/1.2-old/spec.html#id2804923
        if (!property.IsScalar () || property.Tag () != "!")
            throw parse_error (properties, "non-string property specified");

        std::string raw = property.as<std::string> ();
        std::string prop = raw;

        term t;
        if (!prop.empty () && prop[0] == '^') {
            t.negate = true;
            prop = prop.substr (1);
        }
        if (prop.find_first_of ("!&\'\"^`|()") != std::string::npos) {
            std::string errmsg = raw + " is invalid";
            throw parse_error (properties, errmsg.c_str ());
        }

        //  Optional "name=value" form: match a specific value, not just the
        //  presence of a name. Plain "name" tokens (the RFC 31 default) carry
        //  no '=' and keep presence-only semantics, so this is backward
        //  compatible. The query strings are interned once here.
        auto eq = prop.find ('=');
        if (eq != std::string::npos) {
            t.key = Flux::resource_model::property_istr_t{std::string_view{prop}.substr (0, eq)};
            t.value = Flux::resource_model::property_istr_t{std::string_view{prop}.substr (eq + 1)};
            t.has_value = true;
        } else {
            t.key = Flux::resource_model::property_istr_t{prop};
        }
        m_terms.push_back (t);

        //  Preserve the original token for lossless as_yaml() round-tripping.
        values.push_back (raw);
    }
}

bool PropertyConstraint::match (const Flux::resource_model::resource_t &r) const
{
    for (auto &&t : m_terms) {
        //  Interned lookups: no allocation, no hashing of raw text, O(1) each.
        bool found =
            t.has_value ? r.properties.contains (t.key, t.value) : r.properties.contains (t.key);
        if (t.negate)
            found = !found;
        if (!found)
            return false;
    }
    return true;
}

YAML::Node PropertyConstraint::as_yaml () const
{
    YAML::Node node;
    for (auto &&property : values)
        node["properties"].push_back (property);
    return node;
}

// ConditionalConstraint implementation

ConditionalConstraint::ConditionalConstraint (std::string &operation, const YAML::Node &vals)
{
    op = operation;
    for (auto &&constraint : vals)
        values.emplace_back (constraint_parser (constraint));
}

bool ConditionalConstraint::match_and (const Flux::resource_model::resource_t &r) const
{
    for (auto &&constraint : values) {
        if (!constraint->match (r))
            return false;
    }
    return true;
}

bool ConditionalConstraint::match_or (const Flux::resource_model::resource_t &r) const
{
    for (auto &&constraint : values) {
        if (constraint->match (r))
            return true;
    }
    return false;
}

bool ConditionalConstraint::match (const Flux::resource_model::resource_t &r) const
{
    if (op == "and")
        return match_and (r);
    if (op == "or")
        return match_or (r);
    if (op == "not")
        return !match_and (r);
    return false;
}

YAML::Node ConditionalConstraint::as_yaml () const
{
    YAML::Node node;
    for (auto &&constraint : values)
        node[op].push_back (constraint->as_yaml ());
    return node;
}
