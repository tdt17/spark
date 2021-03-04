
# Contributing to Palantir-Spark
 

Palantir's Spark form is set up to track upstream, taking releases on a regular basis. 
We maintain a small set of diffs with upstream, all of which are documented in our FORK.md.
Additions to our fork that don't make it into upstream make the process of taking new releases
more difficult and time-consuming. For this reason we add to our diff with upstream only when strictly necessary.

New Feature Development
---

If you would like to add a feature to our fork, we request that you first make a contribution upstream
([Contributing to Spark guide](https://spark.apache.org/contributing.html).) In the event that this feature 
is refused, (or the PR goes stale) Palantir's Spark team will make a judgment as to whether we can add this 
feature to our diff. We realize that there is a barrier to entry here, but in the past we've been more 
laissez-faire about additions to the fork; the result was a giant, undocumented mess that blocked 
upgrades and took months to untangle. With this tradeoff we get upstream features and fixes sooner, a small well-understood diff, and an
outsourced (rather high) quality bar for new features at the cost of more higher-lift experimentation. 

Correctness Issues
---
Correctness issues can be urgent; contributing to upstream can take a long time. If you encounter a correctness issue,
please bring it to the attention of Palantir's Spark team, and we will work out a remediation plan as soon as possible.
This may include taking a fix and pushing upstream in parallel or removing it at a
later date once upstream sorts out the issue.

Cherry-Picks
---
A similar rationale applies to cherry-picks. The bigger our diff becomes, the harder it is to manage,
to reason about correctness, and to upgrade. Cherry-picks come with their own risk when taken out of context.
As such we prefer regularly taking upstream releases over cherry-picks.

Between releases, we welcome clean-cut cherry-picks of upstream fixes.
Consult Palantir's Spark team if you want to cherry-pick features. 
