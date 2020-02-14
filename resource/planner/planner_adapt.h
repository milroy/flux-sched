/*****************************************************************************\
 *  Copyright (c) 2014 Lawrence Livermore National Security, LLC.  Produced at
 *  the Lawrence Livermore National Laboratory (cf, AUTHORS, DISCLAIMER.LLNS).
 *  LLNL-CODE-658032 All rights reserved.
 *
 *  This file is part of the Flux resource manager framework.
 *  For details, see https://github.com/flux-framework.
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the license, or (at your option)
 *  any later version.
 *
 *  Flux is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the IMPLIED WARRANTY OF MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the terms and conditions of the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.
 *  See also:  http://www.gnu.org/licenses/
\*****************************************************************************/

#ifndef PLANNER_ADAPT_H
#define PLANNER_ADAPT_H

#include "planner.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct planner_adapt planner_adapt_t;

/*! Construct a planner_adapt_t contex that creates and manages len number of
 *  planner_t objects. Individual planner_t context can be accessed via
 *  planner_adapt_at (i). Index i corresponds to the resource type of
 *  i^th element of resource_types array.
 *
 *  \param base_time    earliest schedulable point expressed in integer time
 *                      (i.e., the base time of the planner to be constructed).
 *  \param duration     time span of this planner_adapt (i.e., all planned spans
 *                      must end before base_time + duration).
 *  \param resource_totals
 *                      64-bit unsigned integer array of size len where each
 *                      element contains the total count of available resources
 *                      of a single resource type.
 *  \param resource_types
 *                      string array of size len where each element contains
 *                      the resource type corresponding to each corresponding
 *                      element in the resource_totals array.
 *  \param job_types
 *                      string array of size len where each element contains
 *                      the job type corresponding to each corresponding
 *                      element in the resource_totals array.
 *  \param len          length of the resource_totals and job_types arrays.
 *
 *  \return             a new planner_adapt context; NULL on error with errno
 *                      set as follows:
 *                          EINVAL: invalid argument.
 *                          ERANGE: resource_totals contains an out-of-range
 *                                  value.
 */
planner_adapt_t *planner_adapt_new (int64_t base_time, uint64_t duration,
                                    const uint64_t total_resources,
                                    const char *resource_type,
                                    const char **job_types, size_t len);

/*! Getters:
 *  \return             -1 or NULL on an error with errno set as follows:
 *                         EINVAL: invalid argument.
 */
int64_t planner_adapt_base_time (planner_adapt_t *ctx);
int64_t planner_adapt_duration (planner_adapt_t *ctx);
size_t planner_adapt_resources_len (planner_adapt_t *ctx);
const char **planner_adapt_resource_type (planner_adapt_t *ctx);
const char **planner_adapt_job_types (planner_adapt_t *ctx);
const uint64_t *planner_adapt_total_resources (planner_adapt_t *ctx);
int64_t planner_adapt_resource_total_at (planner_adapt_t *ctx, unsigned int i);
int64_t planner_adapt_resource_total_by_type (planner_adapt_t *ctx,
                                              const char *resource_type);

/*! Reset the planner_adapt_t context with a new time bound.
 *  Destroy all existing planned spans in its managed planner_t objects.
 *
 *  \param ctx          an opaque planner_adapt_t context returned from
 *                      planner_adapt_new.
 *  \param base_time    the earliest schedulable point expressed in integer time
 *                      (i.e., the base time of the planner to be constructed).
 *  \param duration     the time span of this planner (i.e., all planned spans
 *                      must end before base_time + duration).
 *  \return             0 on success; -1 on error with errno set as follows:
 *                          EINVAL: invalid argument.
 */
int planner_adapt_reset (planner_adapt_t *ctx, int64_t base_time,
                         uint64_t duration);

/*! Destroy the planner_adapt.
 *
 *  \param ctx_p        a pointer to a planner_adapt_t context pointer returned
 *                      from planner_new
 *
 */
void planner_adapt_destroy (planner_adapt_t **ctx_p);

/*! Return the i^th planner object managed by the planner_adapt_t ctx.
 *  Index i corresponds to the resource type of i^th element of
 *  resource_types array passed in via planner_adapt_new ().
 *
 *  \param ctx          an opaque planner_adapt_t context returned from
 *                      planner_adapt_new.
 *  \param i            planner array index
 *  \return             a planner_t context pointer on success; NULL on error
 *                      with errno set as follows:
 *                          EINVAL: invalid argument.
 */
planner_t *planner_adapt_planner_at (planner_adapt_t *ctx, unsigned int i);

/*! Find and return the earliest point in integer time when the request can be
 *  satisfied.
 *
 *  Note on semantics: this function returns a time point where the resource state
 *  changes. If the number of available resources change at time point t1 and
 *  t2 (assuming t2 is greater than t1+2), the possible schedule points that this
 *  function can return is either t1 or t2, but not t1+1 nor t1+2 even if these
 *  points also satisfy the request.
 *
 *  \param ctx          an opaque planner_adapt_t context returned from
 *                      planner_adapt_new.
 *  \param on_or_after  available on or after the specified time.
 *  \param duration     requested duration; must be greater than or equal to 1.
 *  \param resource_requests
 *                      64-bit unsigned integer array of size len specifying
 *                      the requested resource counts.
 *  \param len          length of resource_counts and resource_types arrays.
 *  \return             the earliest time at which the resource request
 *                      can be satisfied;
 *                      -1 on error with errno set as follows:
 *                          EINVAL: invalid argument.
 *                          ERANGE: resource_counts contain an out-of-range value.
 *                          ENOENT: no scheduleable point
 */
int64_t planner_adapt_avail_time_first (planner_adapt_t *ctx,
                                        int64_t on_or_after, uint64_t duration,
                                        const uint64_t *resource_requests, size_t len);

/*! Find and return the next earliest point in time at which the same request
 *  queried before via either planner_avail_time_first or
 *  planner_adapt_avail_time_next can be satisfied.  Same semantics as
 *  planner_adapt_avail_time_first.
 *
 *  \param ctx          an opaque planner_adapt_t context returned from
 *                      planner_adapt_new.
 *  \return             the next earliest time at which the resource request
 *                      can be satisfied;
 *                      -1 on error with errno set as follows:
 *                          EINVAL: invalid argument.
 *                          ERANGE: request out of range
 *                          ENOENT: no scheduleable point
 */
int64_t planner_adapt_avail_time_next (planner_adapt_t *ctx);


/*! Return how many resources of ith type is available at the given time.
 *
 *  \param ctx          opaque planner context returned from planner_adapt_new.
 *  \param at           instant time for which this query is made.
 *  \param i            index of the resource type to queried
 *  \return             available resource count; -1 on an error with errno set
 *                      as follows:
 *                          EINVAL: invalid argument.
 */
int64_t planner_adapt_avail_resources_at (planner_adapt_t *ctx, int64_t at,
                                          unsigned int i);

/*! Return how many resources are available at the given instant time (at).
 *
 *  \param ctx          an opaque planner_adapt_t context returned from
 *                      planner_adapt_new.
 *  \param at           instant time for which this query is made.
 *  \param resource_counts
 *                      resources array buffer to copy and return available
 *                      counts into.
 *  \param len          length of resources array.
 *  \return             0 on success; -1 on error with errno set as follows:
 *                          EINVAL: invalid argument.
 */
int planner_adapt_avail_resources_array_at (planner_adapt_t *ctx, int64_t at,
                                            int64_t *resource_counts, size_t len);

/*! Test if the given resource request can be satisfied at the start time.
 *  Note on semantics: Unlike planner_adapt_avail_time* functions, this function
 *  can be used to test an arbitrary time span.
 *
 *  \param ctx          an opaque planner_adapt_t context returned from
 *                      planner_adapt_new.
 *  \param at           start time from which the requested resources must
 *                      be available for duration.
 *  \param duration     requested duration; must be greater than or equal to 1.
 *  \param resource_requests
 *                      64-bit unsigned integer array of size len specifying
 *                      the requested resource counts.
 *  \param len          length of resource_counts and resource_types arrays.
 *  \return             0 if the request can be satisfied; -1 if it cannot
 *                      be satisfied or an error encountered (errno as follows):
 *                          EINVAL: invalid argument.
 *                          ERANGE: resource_counts contain an out-of-range value.
 *                          ENOTSUP: internal error encountered.
 */
int planner_adapt_avail_during (planner_adapt_t *ctx, int64_t at,
                                uint64_t duration,
                                const uint64_t resource_request, size_t len);


/*! Return how many resources are available for the duration starting from at.
 *
 *  \param ctx          an opaque planner_adapt_t context returned from
 *                      planner_adapt_new.
 *  \param at           instant time for which this query is made.
 *  \param duration     requested duration; must be greater than or equal to 1.
 *  \param resource_counts
 *                      resources array buffer to copy and return available counts
 *                      into.
 *  \param len          length of resource_counts and resource_types arrays.
 *  \return             0 on success; -1 on an error with errno set as follows:
 *                          EINVAL: invalid argument.
 */

int planner_adapt_avail_resources_during (planner_adapt_t *ctx,
                                                int64_t at, uint64_t duration,
                                                const char *jobtype);

/*! Add a new span to the multi-planner and update the planner's resource/time
 *  state. Reset the multi-planner's iterator so that
 *  planner_adapt_avail_time_next will be made to return the earliest
 *  schedulable point.
 *
 *  \param ctx          opaque planner_adapt_t context returned
 *                      from planner_adapt_new.
 *  \param start_time   start time from which the resource request must
 *                      be available for duration.
 *  \param duration     requested duration; must be greater than or equal to 1.
 *  \param resource_requests
 *                      resource counts request.
 *  \param len          length of requests.
 *
 *  \return             span id on success; -1 on error with errno set
 *                      as follows:
 *                          EINVAL: invalid argument.
 *                          EKEYREJECTED: can't update planner's internal data.
 *                          ERANGE: a resource state became out of a valid
 *                                  range, e.g., reserving more than available.
 */

int64_t planner_adapt_add_span (planner_adapt_t *ctx, int64_t start_time,
                                uint64_t duration,
                                const uint64_t resource_requests,
                                const char *jobtype);

/*! Remove the existing span from multi-planner and update resource/time state.
 *  Reset the planner's iterator such that planner_avail_time_next will be made
 *  to return the earliest schedulable point.
 *
 *  \param ctx          opaque multi-planner context returned
 *                      from planner_adapt_new.
 *  \param span_id      span_id returned from planner_adapt_add_span.
 *  \return             0 on success; -1 on error with errno set as follows:
 *                          EINVAL: invalid argument.
 *                          EKEYREJECTED: span could not be removed from
 *                                        the planner's internal data structures.
 *                          ERANGE: a resource state became out of a valid range.
 */

int planner_adapt_rem_span_by_jobtype (planner_adapt_t *ctx, int64_t span_id,
                              const char *jobtype);

//! Span iterators -- there is no specific iteration order
//  return -1 when you no longer can iterate: EINVAL when ctx is NULL.
//  ENOENT when you reached the end of the spans
int64_t planner_adapt_span_first (planner_adapt_t *ctx);
int64_t planner_adapt_span_next (planner_adapt_t *ctx);

size_t planner_adapt_span_size (planner_adapt_t *ctx);

#ifdef __cplusplus
}
#endif

#endif /* PLANNER_ADAPT_H */

/*
 * vi: ts=4 sw=4 expandtab
 */
