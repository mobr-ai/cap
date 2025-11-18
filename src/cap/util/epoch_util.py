from datetime import datetime, timezone, timedelta

# Mainnet genesis / system start
SYSTEM_START = datetime(2017, 9, 23, 21, 44, 51, tzinfo=timezone.utc)

# Byron-era parameters (mainnet)
BYRON_SLOTS_PER_EPOCH = 21_600    # slots per epoch in Byron-era sources
BYRON_SLOT_LENGTH_SEC = 20       # seconds per slot in Byron

# Shelley+ era parameters (mainnet; Shelley, Allegra, Mary, Alonzo, Babbage, ...)
SHELLEY_SLOTS_PER_EPOCH = 432_000
SHELLEY_SLOT_LENGTH_SEC = 1

# Era boundary: epoch where Shelley starts on mainnet
SHELLEY_START_EPOCH = 208

# Derived epoch durations in seconds (kept explicit)
BYRON_EPOCH_SECONDS = BYRON_SLOTS_PER_EPOCH * BYRON_SLOT_LENGTH_SEC
SHELLEY_EPOCH_SECONDS = SHELLEY_SLOTS_PER_EPOCH * SHELLEY_SLOT_LENGTH_SEC

def epoch_to_date(epoch: int) -> str:
    """
    Convert an *absolute* Cardano epoch number to a YYYY-MM-DD date (UTC),
    while explicitly checking eras (Byron vs Shelley+).

    Behaviour:
      - For epoch < SHELLEY_START_EPOCH: treat as Byron-era epochs.
      - For epoch >= SHELLEY_START_EPOCH: accumulate Byron-era epochs up to
        the Shelley boundary, then add subsequent Shelley-era epochs.

    Raises:
      - TypeError if epoch is not an int.
      - ValueError if epoch < 0.
    """
    if not isinstance(epoch, int):
        raise TypeError("epoch must be an integer")
    if epoch < 0:
        raise ValueError("epoch must be >= 0")

    # If the requested epoch is fully inside Byron-era
    if epoch < SHELLEY_START_EPOCH:
        seconds_since_genesis = epoch * BYRON_EPOCH_SECONDS
    else:
        # Sum entirety of all Byron epochs up to the Shelley start,
        # then add the number of Shelley epochs after that boundary.
        seconds_before_shelley = SHELLEY_START_EPOCH * BYRON_EPOCH_SECONDS
        shelley_epochs_after = epoch - SHELLEY_START_EPOCH
        seconds_after = shelley_epochs_after * SHELLEY_EPOCH_SECONDS
        seconds_since_genesis = seconds_before_shelley + seconds_after

    dt = SYSTEM_START + timedelta(seconds=seconds_since_genesis)
    return dt.date().isoformat()
