"""Following a polled sign-in, such as an OAuth device flow (RFC 8628).

Relay polls at the step's ``poll_interval_s``, slows down when the provider
says so, and when the code expires (the provider answers ``expired``, or the
step's ``expires_at`` passes) it starts the sign-in again for a new code. None
of this limits how long the person takes.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime

from clio_relay.identity.base import IdentityProvider, SignInContext, SignInResult, SignInStep
from clio_relay.jobs.refusals import Refusal

DEFAULT_POLL_S = 5.0
SLOW_DOWN_STEP_S = 5.0


def polls(step: SignInStep) -> bool:
    """Whether relay polls the provider to learn that this step is done.

    A step with ``poll_interval_s`` is a device flow and is polled whichever
    page it points at. A step on relay's page with no code box is polled too.
    """
    return step.poll_interval_s is not None or (step.relay_page and not step.accepts_code)


def code_expired(step: SignInStep, now: datetime | None = None) -> bool:
    """Whether the step's code has stopped working."""
    if step.expires_at is None:
        return False
    expires = step.expires_at
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=UTC)
    return (now or datetime.now(UTC)) >= expires


async def follow(
    identity: IdentityProvider,
    ctx: SignInContext,
    step: SignInStep,
    *,
    running: Callable[[], bool] = lambda: True,
    on_new_step: Callable[[SignInStep], None] | None = None,
    on_error: Callable[[Refusal], None] | None = None,
) -> SignInResult | None:
    """Poll until the sign-in is done or failed. Returns None once ``running`` is false.

    A provider that cannot be reached for a moment is polled again. A refusal
    while asking for a new code ends the sign-in, since nothing would change.
    """
    ctx.step = step
    interval = step.poll_interval_s or DEFAULT_POLL_S
    while running():
        await asyncio.sleep(interval)
        if not running():
            return None
        expired = code_expired(step)
        if not expired:
            try:
                result = await identity.poll(ctx, step)
            except Refusal as refusal:
                if on_error is not None:
                    on_error(refusal)
                continue
            if result.state == "pending":
                continue
            if result.state == "slow_down":
                interval += SLOW_DOWN_STEP_S
                continue
            if result.state != "expired":
                return result
        step = await identity.begin(ctx)
        ctx.step = step
        interval = step.poll_interval_s or DEFAULT_POLL_S
        if on_new_step is not None:
            on_new_step(step)
    return None
