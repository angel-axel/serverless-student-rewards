# Student Builder Group Rewards

A serverless platform for student communities: members sign up with their
institutional email, attend events to earn points, and redeem those points for
rewards. Leaders manage events, rewards and redemptions from an admin panel,
and subscribers get email notices and day-of reminders.

## Architecture

| Service | Role |
|---|---|
| Amazon Cognito | Sign-up, login (OAuth code flow) and the `administrador` / `miembro` groups |
| API Gateway (HTTP API) | 26 routes, JWT authorizer backed by the user pool |
| AWS Lambda (Python 3.13, arm64) | 5 API functions, 2 Cognito triggers, 1 notifier |
| Amazon DynamoDB | Single table with one GSI, TTL and Streams |
| EventBridge Scheduler | One-off reminders on the day of each event |
| Amazon SES | Verification codes, event notices and messages to the leader |
| Amazon S3 + CloudFront | Private bucket for the frontend and reward images, served through CloudFront |

Each function has its own IAM role with only the actions it uses.

## Prerequisites

- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) and AWS credentials
- A domain verified as an SES identity in the target region, out of the SES sandbox

## Deploy

```bash
sam build
sam deploy --guided
```

| Parameter | Description |
|---|---|
| `GroupName` | Display name for the user pool, the API and the emails |
| `SiteUrl` | Optional. Custom frontend origin, e.g. `https://example.com`. Leave empty to use the CloudFront domain created by the stack |
| `EmailDomain` | Domain verified in SES; mail is sent from `no-reply@` and `mail@` |
| `InstitutionalDomain` | Only emails from this domain can sign up, e.g. `example.edu` |
| `LeaderName` | Group leader's name, shown on redemption coupons |
| `LeaderEmail` | Mailbox that receives messages sent to the leader |
| `AuthDomainPrefix` | Prefix for the Cognito login domain (unique per region) |
| `WelcomeBonus` | `true` to grant the welcome points bonus to new members |

## After deploying

The stack outputs `SiteUrl`, `SiteBucketName`, `ApiUrl`, `UserPoolId`,
`UserPoolClientId` and `LoginDomain`.

### Frontend

`site/index.html` is the whole frontend: a single self-contained page (Spanish
and English) with the member view, the leader dashboard and the voucher
verifier. Fill in the `CONFIG` block at the top of its script with the stack
outputs and your group, institution and email domain, then upload it:

```bash
aws s3 sync site/ s3://<SiteBucketName>
```

The site is then live at the `SiteUrl` output. To serve it from your own
domain, add the alias and an ACM certificate to the distribution and redeploy
with the `SiteUrl` parameter set to that domain.

New users join the `miembro` group automatically. To make someone an admin:

```bash
aws cognito-idp admin-add-user-to-group \
  --user-pool-id <UserPoolId> --username <email> --group-name administrador
```

## API

| Function | Routes |
|---|---|
| `src/events` | `GET /events`, `GET /events/{event_id}`, `GET·PUT /me/subscription`, `GET·POST /admin/events`, `PATCH·DELETE /admin/events/{event_id}`, `GET·POST /admin/events/{event_id}/attendance` |
| `src/rewards` | `GET /rewards`, `GET·POST /admin/rewards`, `PATCH /admin/rewards/{reward_id}`, `POST /admin/uploads` |
| `src/redemptions` | `POST /redemptions`, `GET /me/redemptions`, `GET /coupons/{coupon_id}`, `GET /admin/redemptions`, `PATCH /admin/redemptions/{redemption_id}` |
| `src/users` | `GET /me`, `GET /me/activities` |
| `src/suggestions` | `POST /suggestions`, `POST /messages`, `GET /admin/suggestions`, `GET /admin/messages` |

`GET /events`, `GET /events/{event_id}`, `GET /rewards` and
`GET /coupons/{coupon_id}` are public; every other route requires a Cognito
token, and `/admin/*` routes also check the `administrador` group inside the
function.

## Notes

- Sign-up accepts only student addresses: the local part of the email must
  contain a digit (the student ID). Adjust `src/cognito_pre_signup` if your
  institution uses a different format.
- Times are handled in Mexico City time (UTC-6); change `MX_TZ` in the
  functions for another zone.
- The DynamoDB table, the user pool and the site bucket are retained when the
  stack is deleted, so member data is never removed by accident.

## Disclaimer

This project is not affiliated with, sponsored or endorsed by Amazon Web
Services. AWS and Amazon Web Services are trademarks of Amazon.com, Inc. or
its affiliates.
