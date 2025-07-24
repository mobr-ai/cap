"""
Database models for cardano-db-sync schema.
Map to the actual cardano-db-sync database tables.
"""

from sqlalchemy import (
    Column, Integer, BigInteger, String, Text, DateTime, Boolean,
    Numeric, ForeignKey, Index, LargeBinary, SmallInteger
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Block(Base):
    __tablename__ = 'block'

    id = Column(BigInteger, primary_key=True)
    hash = Column(LargeBinary, nullable=False, unique=True)
    epoch_no = Column(Integer)
    slot_no = Column(BigInteger)
    epoch_slot_no = Column(Integer)
    block_no = Column(Integer)
    previous_id = Column(BigInteger, ForeignKey('block.id'))
    slot_leader_id = Column(BigInteger, ForeignKey('slot_leader.id'))
    size = Column(Integer)
    time = Column(DateTime)
    tx_count = Column(BigInteger)
    proto_major = Column(Integer)
    proto_minor = Column(Integer)
    vrf_key = Column(String)
    op_cert = Column(LargeBinary)
    op_cert_counter = Column(BigInteger)

    # Relationships
    previous_block = relationship("Block", remote_side=[id])
    slot_leader = relationship("SlotLeader")
    transactions = relationship("Tx", back_populates="block")

class SlotLeader(Base):
    __tablename__ = 'slot_leader'

    id = Column(BigInteger, primary_key=True)
    hash = Column(LargeBinary, nullable=False, unique=True)
    pool_hash_id = Column(BigInteger, ForeignKey('pool_hash.id'))
    description = Column(String)

    pool_hash = relationship("PoolHash")

class PoolHash(Base):
    __tablename__ = 'pool_hash'

    id = Column(BigInteger, primary_key=True)
    hash_raw = Column(LargeBinary, nullable=False, unique=True)
    view = Column(String, nullable=False, unique=True)

class Tx(Base):
    __tablename__ = 'tx'

    id = Column(BigInteger, primary_key=True)
    hash = Column(LargeBinary, nullable=False, unique=True)
    block_id = Column(BigInteger, ForeignKey('block.id'), nullable=False)
    block_index = Column(Integer, nullable=False)
    out_sum = Column(Numeric(20, 0))
    fee = Column(Numeric(20, 0))
    deposit = Column(BigInteger)
    size = Column(Integer)
    invalid_before = Column(BigInteger)
    invalid_hereafter = Column(BigInteger)
    valid_contract = Column(Boolean)
    script_size = Column(Integer)

    # Relationships
    block = relationship("Block", back_populates="transactions")
    inputs = relationship("TxIn", back_populates="tx")
    outputs = relationship("TxOut", back_populates="tx")
    metadata = relationship("TxMetadata", back_populates="tx")
    minted_assets = relationship("MaTxMint", back_populates="tx")

class TxIn(Base):
    __tablename__ = 'tx_in'

    id = Column(BigInteger, primary_key=True)
    tx_in_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)
    tx_out_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)
    tx_out_index = Column(SmallInteger, nullable=False)
    redeemer_id = Column(BigInteger, ForeignKey('redeemer.id'))

    # Relationships
    tx = relationship("Tx", foreign_keys=[tx_in_id], back_populates="inputs")
    tx_out_ref = relationship("Tx", foreign_keys=[tx_out_id])
    redeemer = relationship("Redeemer")

class TxOut(Base):
    __tablename__ = 'tx_out'

    id = Column(BigInteger, primary_key=True)
    tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)
    index = Column(SmallInteger, nullable=False)
    address = Column(String, nullable=False)
    address_raw = Column(LargeBinary, nullable=False)
    payment_cred = Column(LargeBinary)
    stake_address_id = Column(BigInteger, ForeignKey('stake_address.id'))
    value = Column(Numeric(20, 0), nullable=False)
    data_hash = Column(LargeBinary)
    inline_datum_id = Column(BigInteger, ForeignKey('datum.id'))
    reference_script_id = Column(BigInteger, ForeignKey('script.id'))

    # Relationships
    tx = relationship("Tx", back_populates="outputs")
    stake_address = relationship("StakeAddress")
    inline_datum = relationship("Datum")
    reference_script = relationship("Script")
    multi_assets = relationship("MaTxOut", back_populates="tx_out")

class StakeAddress(Base):
    __tablename__ = 'stake_address'

    id = Column(BigInteger, primary_key=True)
    hash_raw = Column(LargeBinary, nullable=False, unique=True)
    view = Column(String, nullable=False)
    script_hash = Column(LargeBinary)
    registered_tx_id = Column(BigInteger, ForeignKey('tx.id'))

    registered_tx = relationship("Tx")

class StakeRegistration(Base):
    __tablename__ = 'stake_registration'

    id = Column(BigInteger, primary_key=True)
    addr_id = Column(BigInteger, ForeignKey('stake_address.id'), nullable=False)
    cert_index = Column(Integer, nullable=False)
    tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)

    addr = relationship("StakeAddress")
    tx = relationship("Tx")

class StakeDeregistration(Base):
    __tablename__ = 'stake_deregistration'

    id = Column(BigInteger, primary_key=True)
    addr_id = Column(BigInteger, ForeignKey('stake_address.id'), nullable=False)
    cert_index = Column(Integer, nullable=False)
    tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)

    addr = relationship("StakeAddress")
    tx = relationship("Tx")

class MaTxOut(Base):
    __tablename__ = 'ma_tx_out'

    id = Column(BigInteger, primary_key=True)
    quantity = Column(Numeric(20, 0), nullable=False)
    tx_out_id = Column(BigInteger, ForeignKey('tx_out.id'), nullable=False)
    ident = Column(BigInteger, ForeignKey('multi_asset.id'), nullable=False)

    # Relationships
    tx_out = relationship("TxOut", back_populates="multi_assets")
    multi_asset = relationship("MultiAsset")

class MaTxMint(Base):
    __tablename__ = 'ma_tx_mint'
    id = Column(BigInteger, primary_key=True)
    quantity = Column(Numeric(20, 0), nullable=False)
    tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)
    ident = Column(BigInteger, ForeignKey('multi_asset.id'), nullable=False)

    # Relationships
    tx = relationship("Tx", back_populates="minted_assets")
    multi_asset = relationship("MultiAsset")

class MultiAsset(Base):
    __tablename__ = 'multi_asset'

    id = Column(BigInteger, primary_key=True)
    policy = Column(LargeBinary, nullable=False)
    name = Column(LargeBinary, nullable=False)
    fingerprint = Column(String, nullable=False)

    __table_args__ = (
        Index('unique_multi_asset', 'policy', 'name', unique=True),
    )

class TxMetadata(Base):
    __tablename__ = 'tx_metadata'

    id = Column(BigInteger, primary_key=True)
    key = Column(Numeric(20, 0), nullable=False)
    json = Column(Text)
    bytes = Column(LargeBinary)
    tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)

    tx = relationship("Tx", back_populates="metadata")

class Epoch(Base):
    __tablename__ = 'epoch'

    id = Column(BigInteger, primary_key=True)
    out_sum = Column(Numeric(20, 0))
    fees = Column(Numeric(20, 0))
    tx_count = Column(Integer)
    blk_count = Column(Integer)
    no = Column(Integer, nullable=False, unique=True)
    start_time = Column(DateTime)
    end_time = Column(DateTime)

class PoolMetadataRef(Base):
    __tablename__ = 'pool_metadata_ref'

    id = Column(BigInteger, primary_key=True)
    pool_id = Column(BigInteger, ForeignKey('pool_hash.id'), nullable=False)
    url = Column(String, nullable=False)
    hash = Column(LargeBinary, nullable=False)
    registered_tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)

    pool = relationship("PoolHash")
    registered_tx = relationship("Tx")

class Script(Base):
    __tablename__ = 'script'

    id = Column(BigInteger, primary_key=True)
    tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)
    hash = Column(LargeBinary, nullable=False, unique=True)
    type = Column(String, nullable=False)  # 'timelock', 'plutusV1', 'plutusV2', etc.
    json = Column(Text)
    bytes = Column(LargeBinary)
    serialised_size = Column(Integer)

    tx = relationship("Tx")

class Datum(Base):
    __tablename__ = 'datum'

    id = Column(BigInteger, primary_key=True)
    hash = Column(LargeBinary, nullable=False, unique=True)
    tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)
    value = Column(Text)
    bytes = Column(LargeBinary)

    tx = relationship("Tx")

class Redeemer(Base):
    __tablename__ = 'redeemer'

    id = Column(BigInteger, primary_key=True)
    tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)
    unit_mem = Column(BigInteger)
    unit_steps = Column(BigInteger)
    fee = Column(Numeric(20, 0))
    purpose = Column(String)  # 'spend', 'mint', 'cert', 'reward'
    index = Column(Integer)
    script_hash = Column(LargeBinary)

    tx = relationship("Tx")

class Delegation(Base):
    __tablename__ = 'delegation'

    id = Column(BigInteger, primary_key=True)
    addr_id = Column(BigInteger, ForeignKey('stake_address.id'), nullable=False)
    cert_index = Column(Integer, nullable=False)
    pool_hash_id = Column(BigInteger, ForeignKey('pool_hash.id'), nullable=False)
    active_epoch_no = Column(BigInteger, nullable=False)
    tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)
    slot_no = Column(BigInteger)
    redeemer_id = Column(BigInteger, ForeignKey('redeemer.id'))

    # Relationships
    addr = relationship("StakeAddress")
    pool_hash = relationship("PoolHash")
    tx = relationship("Tx")
    redeemer = relationship("Redeemer")

class PoolUpdate(Base):
    __tablename__ = 'pool_update'

    id = Column(BigInteger, primary_key=True)
    hash_id = Column(BigInteger, ForeignKey('pool_hash.id'), nullable=False)
    cert_index = Column(Integer, nullable=False)
    vrf_key_hash = Column(LargeBinary, nullable=False)
    pledge = Column(Numeric(20, 0), nullable=False)
    reward_addr = Column(String, nullable=False)
    active_epoch_no = Column(BigInteger, nullable=False)
    meta_id = Column(BigInteger, ForeignKey('pool_metadata_ref.id'))
    margin = Column(Numeric, nullable=False)
    fixed_cost = Column(Numeric(20, 0), nullable=False)
    registered_tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)

    # Relationships
    hash = relationship("PoolHash")
    meta = relationship("PoolMetadataRef")
    registered_tx = relationship("Tx")

class PoolRetire(Base):
    __tablename__ = 'pool_retire'

    id = Column(BigInteger, primary_key=True)
    hash_id = Column(BigInteger, ForeignKey('pool_hash.id'), nullable=False)
    cert_index = Column(Integer, nullable=False)
    announced_tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)
    retiring_epoch = Column(Integer, nullable=False)

    # Relationships
    hash = relationship("PoolHash")
    announced_tx = relationship("Tx")

# Reward and Treasury Classes
class Reward(Base):
    __tablename__ = 'reward'

    id = Column(BigInteger, primary_key=True)
    addr_id = Column(BigInteger, ForeignKey('stake_address.id'), nullable=False)
    type = Column(String, nullable=False)  # 'member', 'leader', 'treasury', 'reserves'
    amount = Column(Numeric(20, 0), nullable=False)
    earned_epoch = Column(BigInteger, ForeignKey('epoch.id'), nullable=False)
    spendable_epoch = Column(BigInteger, ForeignKey('epoch.id'), nullable=False)
    pool_id = Column(BigInteger, ForeignKey('pool_hash.id'))

    # Relationships
    addr = relationship("StakeAddress")
    earned_epoch_ref = relationship("Epoch", foreign_keys=[earned_epoch])
    spendable_epoch_ref = relationship("Epoch", foreign_keys=[spendable_epoch])
    pool = relationship("PoolHash")

class Withdrawal(Base):
    __tablename__ = 'withdrawal'

    id = Column(BigInteger, primary_key=True)
    addr_id = Column(BigInteger, ForeignKey('stake_address.id'), nullable=False)
    tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)
    amount = Column(Numeric(20, 0), nullable=False)

    # Relationships
    addr = relationship("StakeAddress")
    tx = relationship("Tx")

class Treasury(Base):
    __tablename__ = 'treasury'

    id = Column(BigInteger, primary_key=True)
    addr_id = Column(BigInteger, ForeignKey('stake_address.id'), nullable=False)
    cert_index = Column(Integer, nullable=False)
    amount = Column(Numeric(20, 0), nullable=False)
    tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)

    # Relationships
    addr = relationship("StakeAddress")
    tx = relationship("Tx")

class Reserve(Base):
    __tablename__ = 'reserve'

    id = Column(BigInteger, primary_key=True)
    addr_id = Column(BigInteger, ForeignKey('stake_address.id'), nullable=False)
    cert_index = Column(Integer, nullable=False)
    amount = Column(Numeric(20, 0), nullable=False)
    tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)

    # Relationships
    addr = relationship("StakeAddress")
    tx = relationship("Tx")

class PotTransfer(Base):
    __tablename__ = 'pot_transfer'

    id = Column(BigInteger, primary_key=True)
    cert_index = Column(Integer, nullable=False)
    treasury = Column(Numeric(20, 0), nullable=False)
    reserves = Column(Numeric(20, 0), nullable=False)
    tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)

    tx = relationship("Tx")

class InstantaneousReward(Base):
    __tablename__ = 'instant_reward'

    id = Column(BigInteger, primary_key=True)
    addr_id = Column(BigInteger, ForeignKey('stake_address.id'), nullable=False)
    type = Column(String, nullable=False)  # 'reserves', 'treasury'
    amount = Column(Numeric(20, 0), nullable=False)
    tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)

    # Relationships
    addr = relationship("StakeAddress")
    tx = relationship("Tx")

# Governance Classes (Conway Era)
class GovernanceAction(Base):
    __tablename__ = 'gov_action_proposal'

    id = Column(BigInteger, primary_key=True)
    tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)
    index = Column(Integer, nullable=False)
    type = Column(String, nullable=False)
    description = Column(Text)
    deposit = Column(Numeric(20, 0), nullable=False)
    return_address = Column(String, nullable=False)

    tx = relationship("Tx")

class VotingProcedure(Base):
    __tablename__ = 'voting_procedure'

    id = Column(BigInteger, primary_key=True)
    tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)
    index = Column(Integer, nullable=False)
    gov_action_proposal_id = Column(BigInteger, ForeignKey('gov_action_proposal.id'), nullable=False)
    voter_role = Column(String, nullable=False)  # 'ConstitutionalCommittee', 'DRep', 'SPO'
    voter_hash = Column(LargeBinary, nullable=False)
    vote = Column(String, nullable=False)  # 'Yes', 'No', 'Abstain'

    # Relationships
    tx = relationship("Tx")
    gov_action_proposal = relationship("GovernanceAction")

class CommitteeMember(Base):
    __tablename__ = 'committee_member'

    id = Column(BigInteger, primary_key=True)
    hash = Column(LargeBinary, nullable=False, unique=True)
    has_script = Column(Boolean, nullable=False, default=False)

class DrepRegistration(Base):
    __tablename__ = 'drep_registration'

    id = Column(BigInteger, primary_key=True)
    tx_id = Column(BigInteger, ForeignKey('tx.id'), nullable=False)
    cert_index = Column(Integer, nullable=False)
    drep_hash_id = Column(BigInteger, ForeignKey('drep_hash.id'), nullable=False)
    deposit = Column(Numeric(20, 0))
    voting_anchor_id = Column(BigInteger, ForeignKey('voting_anchor.id'))

    # Relationships
    tx = relationship("Tx")
    drep_hash = relationship("DrepHash")
    voting_anchor = relationship("VotingAnchor")

class DrepHash(Base):
    __tablename__ = 'drep_hash'

    id = Column(BigInteger, primary_key=True)
    raw = Column(LargeBinary, nullable=False, unique=True)
    view = Column(String, nullable=False, unique=True)
    has_script = Column(Boolean, nullable=False, default=False)

class VotingAnchor(Base):
    __tablename__ = 'voting_anchor'

    id = Column(BigInteger, primary_key=True)
    url = Column(String, nullable=False)
    data_hash = Column(LargeBinary, nullable=False)

# Add indexes for performance
Index('idx_block_slot_no', Block.slot_no)
Index('idx_block_epoch_no', Block.epoch_no)
Index('idx_tx_block_id', Tx.block_id)
Index('idx_tx_out_tx_id', TxOut.tx_id)
Index('idx_tx_in_tx_id', TxIn.tx_in_id)
Index('idx_ma_tx_out_tx_out_id', MaTxOut.tx_out_id)
Index('idx_delegation_addr_id', Delegation.addr_id)
Index('idx_delegation_pool_hash_id', Delegation.pool_hash_id)
Index('idx_reward_addr_id', Reward.addr_id)
Index('idx_reward_earned_epoch', Reward.earned_epoch)
Index('idx_withdrawal_addr_id', Withdrawal.addr_id)
Index('idx_voting_procedure_gov_action', VotingProcedure.gov_action_proposal_id)