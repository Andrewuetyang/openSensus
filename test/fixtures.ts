import type { Observation } from "../src/protocol.js";

const source = {
  system: "gitlab",
  instance: "acme-gitlab",
};

export const team = {
  type: "organization.team",
  id: "org:acme/team/payments",
} as const;

export const repository = {
  type: "software.repository",
  id: "gitlab:acme/payments-api",
} as const;

export const change = {
  type: "software.change",
  id: "gitlab:acme/payments-api!3812",
} as const;

export function gitlabScenario(): Observation[] {
  return [
    {
      spec_version: "sensus/0.1",
      observation_id: "obs_team",
      tenant_id: "acme",
      kind: "entity.observed",
      subject: team,
      occurred_at: "2026-09-01T00:00:00Z",
      observed_at: "2026-09-01T00:00:01Z",
      source: { system: "hris", instance: "acme-hris" },
      data: {
        name: "Payments",
        lifecycle: "active",
        attributes: { department: "Engineering" },
      },
    },
    {
      spec_version: "sensus/0.1",
      observation_id: "obs_repo",
      tenant_id: "acme",
      kind: "entity.observed",
      subject: repository,
      occurred_at: "2026-09-01T00:00:00Z",
      observed_at: "2026-09-01T00:00:01Z",
      source,
      data: {
        name: "payments-api",
        lifecycle: "active",
        attributes: { default_branch: "main" },
      },
    },
    {
      spec_version: "sensus/0.1",
      observation_id: "obs_repo_owner",
      tenant_id: "acme",
      kind: "relation.observed",
      subject: repository,
      occurred_at: "2026-09-01T00:00:00Z",
      observed_at: "2026-09-01T00:00:01Z",
      source: { system: "service-catalog", instance: "acme-catalog" },
      data: {
        relation: "organization.owned_by",
        target: team,
        status: "active",
      },
    },
    {
      spec_version: "sensus/0.1",
      observation_id: "obs_change",
      tenant_id: "acme",
      kind: "entity.observed",
      subject: change,
      occurred_at: "2026-09-15T09:00:00Z",
      observed_at: "2026-09-15T09:00:02Z",
      source,
      data: {
        name: "Add batch refund support",
        lifecycle: "active",
        attributes: { changed_files: 23 },
      },
      evidence: [
        {
          type: "source_record",
          ref: "gitlab://acme/payments-api/merge_requests/3812",
          resolver: {
            capability: "gitlab.merge_request.read",
            arguments: { project: "acme/payments-api", merge_request_iid: 3812 },
          },
        },
      ],
    },
    {
      spec_version: "sensus/0.1",
      observation_id: "obs_change_repo",
      tenant_id: "acme",
      kind: "relation.observed",
      subject: change,
      occurred_at: "2026-09-15T09:00:00Z",
      observed_at: "2026-09-15T09:00:02Z",
      source,
      data: {
        relation: "software.belongs_to",
        target: repository,
        status: "active",
      },
    },
    {
      spec_version: "sensus/0.1",
      observation_id: "obs_review_requested",
      tenant_id: "acme",
      kind: "event.occurred",
      subject: change,
      occurred_at: "2026-09-15T09:05:00Z",
      observed_at: "2026-09-15T09:05:02Z",
      source,
      data: {
        type: "software.review_requested",
        attributes: { reviewer_count: 2 },
      },
    },
    {
      spec_version: "sensus/0.1",
      observation_id: "obs_review_state",
      tenant_id: "acme",
      kind: "state.observed",
      subject: change,
      occurred_at: "2026-09-15T09:05:00Z",
      observed_at: "2026-09-15T09:05:02Z",
      source,
      data: {
        field: "software.review_status",
        operation: "set",
        value: "waiting",
        previous_value: "not_requested",
      },
    },
    {
      spec_version: "sensus/0.1",
      observation_id: "obs_review_baseline",
      tenant_id: "acme",
      kind: "metric.observed",
      subject: team,
      occurred_at: "2026-09-09T00:00:00Z",
      observed_at: "2026-09-09T00:00:05Z",
      source: { system: "sensus-runtime", instance: "acme" },
      data: {
        metric: "software.review_wait_time",
        value: 3.2,
        unit: "hour",
        interval: {
          from: "2026-09-02T00:00:00Z",
          to: "2026-09-09T00:00:00Z",
        },
        dimensions: { repository: "payments-api" },
        aggregation: "average",
      },
    },
    {
      spec_version: "sensus/0.1",
      observation_id: "obs_review_current",
      tenant_id: "acme",
      kind: "metric.observed",
      subject: team,
      occurred_at: "2026-09-16T00:00:00Z",
      observed_at: "2026-09-16T00:00:05Z",
      source: { system: "sensus-runtime", instance: "acme" },
      data: {
        metric: "software.review_wait_time",
        value: 7.8,
        unit: "hour",
        interval: {
          from: "2026-09-09T00:00:00Z",
          to: "2026-09-16T00:00:00Z",
        },
        dimensions: { repository: "payments-api" },
        aggregation: "average",
      },
    },
  ];
}

