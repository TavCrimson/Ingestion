"""Review roles and escalation chain."""

ROLES = ["reviewer", "senior_reviewer", "escalation_owner"]

ESCALATION_CHAIN = {
    "reviewer": "senior_reviewer",
    "senior_reviewer": "escalation_owner",
    "escalation_owner": "escalation_owner",  # top of chain — stays here
}


def next_role(current_role: str) -> str:
    return ESCALATION_CHAIN.get(current_role, "escalation_owner")
