package sparktradestats

import java.sql.Timestamp

/** Only validated, deduplicated records become Trades. All timestamps are UTC. */
final case class Trade(
    trade_id: String,
    timestamp: Timestamp,
    instrument: String,
    side: String,
    price: BigDecimal,
    quantity: Long,
    venue: String,
    clearing_member: String
)
