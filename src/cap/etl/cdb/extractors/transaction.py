from typing import Any, Optional, Iterator
from sqlalchemy.orm import joinedload
from sqlalchemy import func
from opentelemetry import trace
import logging

from cap.etl.cdb.extractors.extractor import BaseExtractor
from cap.data.cdb_model import Tx, TxIn, TxOut, MaTxOut, MaTxMint, TxMetadata

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class TransactionExtractor(BaseExtractor):
    """Extractor for transaction data with complete data coverage."""

    def extract_batch(self, last_processed_id: Optional[int] = None) -> Iterator[list[dict[str, Any]]]:
        """Extract transactions in batches with all related data."""
        with tracer.start_as_current_span("transaction_extraction") as span:
            query = self.db_session.query(Tx).options(
                joinedload(Tx.block),
                joinedload(Tx.inputs).joinedload(TxIn.redeemer),
                joinedload(Tx.outputs).joinedload(TxOut.stake_address),
                joinedload(Tx.outputs).joinedload(TxOut.multi_assets).joinedload(MaTxOut.multi_asset),
                joinedload(Tx.outputs).joinedload(TxOut.inline_datum),
                joinedload(Tx.outputs).joinedload(TxOut.reference_script),
                joinedload(Tx.minted_assets).joinedload(MaTxMint.multi_asset),
                joinedload(Tx.metadata)
            )

            if last_processed_id:
                query = query.filter(Tx.id > last_processed_id)

            query = query.order_by(Tx.id)

            offset = 0
            while True:
                batch = query.offset(offset).limit(self.batch_size).all()
                if not batch:
                    break

                span.set_attribute("batch_size", len(batch))
                span.set_attribute("offset", offset)

                yield [self._serialize_transaction(tx) for tx in batch]
                offset += self.batch_size

    def _serialize_transaction(self, tx: Tx) -> dict[str, Any]:
        """Serialize a transaction to dictionary with complete data."""
        return {
            'id': tx.id,
            'hash': tx.hash.hex() if tx.hash else None,
            'block_id': tx.block_id,
            'block_hash': tx.block.hash.hex() if tx.block and tx.block.hash else None,
            'block_timestamp': tx.block.time.isoformat() if tx.block and tx.block.time else None,
            'block_epoch_no': tx.block.epoch_no if tx.block else None,  # ADD THIS
            'block_index': tx.block_index,
            'out_sum': str(tx.out_sum) if tx.out_sum else None,
            'fee': str(tx.fee) if tx.fee else None,
            'deposit': str(tx.deposit) if tx.deposit else None,
            'size': tx.size,
            'invalid_before': tx.invalid_before,
            'invalid_hereafter': tx.invalid_hereafter,
            'valid_contract': tx.valid_contract,
            'script_size': tx.script_size,
            'inputs': [self._serialize_tx_input(inp) for inp in tx.inputs],
            'outputs': [self._serialize_tx_output(out) for out in tx.outputs],
            'metadata': [self._serialize_tx_metadata(meta) for meta in tx.metadata],
            'minted_assets': [self._serialize_minted_asset(ma) for ma in getattr(tx, 'minted_assets', [])],
            'certificates': self._extract_certificates(tx),
            'withdrawals': self._extract_withdrawals(tx)
        }

    def _serialize_tx_input(self, tx_in: TxIn) -> dict[str, Any]:
        """Serialize transaction input."""
        return {
            'id': tx_in.id,
            'tx_out_id': tx_in.tx_out_id,
            'tx_out_index': tx_in.tx_out_index,
            'redeemer_id': tx_in.redeemer_id,
            'redeemer_purpose': tx_in.redeemer.purpose if tx_in.redeemer else None,
            'redeemer_unit_mem': tx_in.redeemer.unit_mem if tx_in.redeemer else None,
            'redeemer_unit_steps': tx_in.redeemer.unit_steps if tx_in.redeemer else None,
            'redeemer_fee': str(tx_in.redeemer.fee) if tx_in.redeemer and tx_in.redeemer.fee else None,
            'redeemer_script_hash': tx_in.redeemer.script_hash.hex() if tx_in.redeemer and tx_in.redeemer.script_hash else None
        }

    def _serialize_tx_output(self, tx_out: TxOut) -> dict[str, Any]:
        """Serialize transaction output including datums."""
        return {
            'id': tx_out.id,
            'index': tx_out.index,
            'address': tx_out.address,
            'address_raw': tx_out.address_raw.hex() if tx_out.address_raw else None,
            'payment_cred': tx_out.payment_cred.hex() if tx_out.payment_cred else None,
            'stake_address_id': tx_out.stake_address_id,
            'stake_address': tx_out.stake_address.view if tx_out.stake_address else None,
            'value': str(tx_out.value),
            'data_hash': tx_out.data_hash.hex() if tx_out.data_hash else None,
            'inline_datum_id': tx_out.inline_datum_id,
            'inline_datum_hash': tx_out.inline_datum.hash.hex() if tx_out.inline_datum and tx_out.inline_datum.hash else None,
            'inline_datum_value': tx_out.inline_datum.value if tx_out.inline_datum else None,
            'reference_script_id': tx_out.reference_script_id,
            'reference_script_hash': tx_out.reference_script.hash.hex() if tx_out.reference_script and tx_out.reference_script.hash else None,
            'reference_script_type': tx_out.reference_script.type if tx_out.reference_script else None,
            'multi_assets': [self._serialize_multi_asset_output(ma) for ma in tx_out.multi_assets]
        }

    def _serialize_multi_asset_output(self, ma_tx_out: MaTxOut) -> dict[str, Any]:
        """Serialize multi-asset transaction output."""
        return {
            'quantity': str(ma_tx_out.quantity),
            'policy': ma_tx_out.multi_asset.policy.hex() if ma_tx_out.multi_asset.policy else None,
            'name': ma_tx_out.multi_asset.name.hex() if ma_tx_out.multi_asset.name else None,
            'name_utf8': ma_tx_out.multi_asset.name.decode('utf-8', errors='ignore') if ma_tx_out.multi_asset.name else None,
            'fingerprint': ma_tx_out.multi_asset.fingerprint
        }

    def _serialize_minted_asset(self, ma_tx_mint: MaTxMint) -> dict[str, Any]:
        """Serialize minted asset."""
        return {
            'quantity': str(ma_tx_mint.quantity),
            'policy': ma_tx_mint.multi_asset.policy.hex() if ma_tx_mint.multi_asset.policy else None,
            'name': ma_tx_mint.multi_asset.name.hex() if ma_tx_mint.multi_asset.name else None,
            'name_utf8': ma_tx_mint.multi_asset.name.decode('utf-8', errors='ignore') if ma_tx_mint.multi_asset.name else None,
            'fingerprint': ma_tx_mint.multi_asset.fingerprint
        }

    def _serialize_tx_metadata(self, metadata: TxMetadata) -> dict[str, Any]:
        """Serialize transaction metadata."""
        return {
            'key': str(metadata.key),
            'json': metadata.json,
            'bytes': metadata.bytes.hex() if metadata.bytes else None
        }

    def _extract_certificates(self, tx: Tx) -> list[dict[str, Any]]:
        """Extract certificate data from transaction."""
        from cap.data.cdb_model import StakeRegistration, StakeDeregistration, Delegation, PoolUpdate, PoolRetire

        certificates = []

        # Get stake registrations
        stake_regs = self.db_session.query(StakeRegistration).filter(
            StakeRegistration.tx_id == tx.id
        ).all()

        for reg in stake_regs:
            certificates.append({
                'type': 'stake_registration',
                'cert_index': reg.cert_index,
                'addr_id': reg.addr_id
            })

        # Get stake deregistrations
        stake_deregs = self.db_session.query(StakeDeregistration).filter(
            StakeDeregistration.tx_id == tx.id
        ).all()

        for dereg in stake_deregs:
            certificates.append({
                'type': 'stake_deregistration',
                'cert_index': dereg.cert_index,
                'addr_id': dereg.addr_id
            })

        # Get delegations
        delegations = self.db_session.query(Delegation).filter(
            Delegation.tx_id == tx.id
        ).all()

        for deleg in delegations:
            certificates.append({
                'type': 'delegation',
                'cert_index': deleg.cert_index,
                'addr_id': deleg.addr_id,
                'pool_hash_id': deleg.pool_hash_id
            })

        # Get pool updates
        pool_updates = self.db_session.query(PoolUpdate).filter(
            PoolUpdate.registered_tx_id == tx.id
        ).all()

        for update in pool_updates:
            certificates.append({
                'type': 'pool_update',
                'cert_index': update.cert_index,
                'hash_id': update.hash_id
            })

        # Get pool retirements
        pool_retires = self.db_session.query(PoolRetire).filter(
            PoolRetire.announced_tx_id == tx.id
        ).all()

        for retire in pool_retires:
            certificates.append({
                'type': 'pool_retire',
                'cert_index': retire.cert_index,
                'hash_id': retire.hash_id,
                'retiring_epoch': retire.retiring_epoch
            })

        return certificates

    def _extract_withdrawals(self, tx: Tx) -> list[dict[str, Any]]:
        """Extract withdrawal data from transaction."""
        from cap.data.cdb_model import Withdrawal

        withdrawals = []

        tx_withdrawals = self.db_session.query(Withdrawal).filter(
            Withdrawal.tx_id == tx.id
        ).all()

        for withdrawal in tx_withdrawals:
            withdrawals.append({
                'id': withdrawal.id,
                'addr_id': withdrawal.addr_id,
                'amount': str(withdrawal.amount)
            })

        return withdrawals

    def get_total_count(self) -> int:
        return self.db_session.query(func.count(Tx.id)).scalar()

    def get_last_id(self) -> Optional[int]:
        result = self.db_session.query(func.max(Tx.id)).scalar()
        return result