SKIPPED_PROCUREMENT_METHOD_TYPES = [
    'competitiveDialogueUA',
    'competitiveDialogueEU',
]
# statuses of tender, in which it will be processed by databridge
TARGET_TENDER_STATUSES = (
    "active.qualification",
    "active",
    "active.awarded",
    "complete"
)
# status of lot, that will be processed by databridge
TARGET_LOT_STATUS = 'complete'
DAYS_PER_YEAR = 365
ACCELERATOR = 86400
