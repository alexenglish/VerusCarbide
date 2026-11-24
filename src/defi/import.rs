//! Backward structural import navigation and strictly evidenced allocations.
use super::*;

pub(super) async fn inspect(configs: &[Config], root: &OutpointRef, seed: &Value) -> Trail {
    let mut trail = Trail::default();
    trail
        .entries
        .push(entry(root, "Import", "Selected currency import"));
    let result = inspect_inner(configs, root, seed, &mut trail).await;
    if let Err(error) = result {
        trail.stop = Some(Stop {
            message: error.to_string(),
        });
    }
    trail
}

async fn inspect_inner(
    configs: &[Config],
    root: &OutpointRef,
    seed: &Value,
    trail: &mut Trail,
) -> anyhow::Result<()> {
    use anyhow::{Context, ensure};
    let config = configs
        .iter()
        .find(|c| c.chain.system_id.get() == Some(&root.system_id))
        .context("No configured source for the importing system")?;
    let client = RpcClient::for_config(&config.noninteractive())?;
    let fresh = client.get_raw_transaction(&root.txid).await?;
    let tx = &fresh.result;
    ensure!(
        tx["txid"].as_str() == Some(&root.txid),
        "Import transaction mismatch"
    );
    ensure!(
        tx["confirmations"].as_i64().is_some_and(|count| count > 0),
        "A confirmed import is required for allocation proof"
    );
    let import = &output(tx, root.vout).context("Import output is absent")?["scriptPubKey"]["crosschainimport"];
    ensure!(
        import.is_object()
            && output(seed, root.vout)
                .is_some_and(|out| out["scriptPubKey"]["crosschainimport"] == *import),
        "Loaded and fresh import payloads differ; refresh the transaction"
    );
    let currency = import["importcurrencyid"]
        .as_str()
        .context("Import currency absent")?;
    let height = if let Some(height) = tx["height"].as_u64() {
        height
    } else {
        let hash = tx["blockhash"]
            .as_str()
            .context("Import block unavailable")?;
        let header = client
            .call_with_raw::<Value>("getblockheader", &[json!(hash), json!(true)])
            .await?;
        ensure!(header.source_id == fresh.source_id, "Import source changed");
        header.result["height"]
            .as_u64()
            .context("Import height unavailable")?
    };
    let records = client
        .call_nullable_with_raw::<Value>(
            "getimports",
            &[json!(currency), json!(height), json!(height)],
        )
        .await?;
    ensure!(
        records.source_id == fresh.source_id,
        "Import source changed"
    );
    let records = records.result.unwrap_or(Value::Null);
    let record = records
        .as_array()
        .into_iter()
        .flatten()
        .find(|record| {
            record["importtxid"].as_str() == Some(&root.txid)
                && record["importvout"].as_u64() == Some(u64::from(root.vout))
        })
        .context("Import record unavailable at its observed height")?;
    ensure!(
        record["import"] == *import,
        "Indexed import payload mismatch"
    );
    let source = import["sourcesystemid"]
        .as_str()
        .context("Source system absent")?;
    let export_id = import["exporttxid"]
        .as_str()
        .context("Export reference absent")?;
    let export_vout = import["exporttxout"]
        .as_u64()
        .and_then(|n| u32::try_from(n).ok())
        .context("Export output reference absent")?;
    let export_ref = OutpointRef {
        system_id: source.to_owned().try_into()?,
        txid: export_id.into(),
        vout: export_vout,
    };
    trail.entries.push(entry(
        &export_ref,
        "Export",
        "Source export named by this import",
    ));
    let source_config = configs
        .iter()
        .find(|c| c.chain.system_id.get() == Some(&export_ref.system_id))
        .context("Export system has no configured source; import reference remains navigable")?;
    let source_client = RpcClient::for_config(&source_config.noninteractive())?;
    let export_tx = source_client.get_raw_transaction(export_id).await?;
    let export = &output(&export_tx.result, export_vout).context("Export output absent")?["scriptPubKey"]
        ["crosschainexport"];
    ensure!(
        export_tx.result["confirmations"]
            .as_i64()
            .is_some_and(|count| count > 0)
            && export_tx.result["txid"].as_str() == Some(export_id)
            && export["sourcesystemid"].as_str() == Some(source)
            && export["destinationsystemid"].as_str() == Some(root.system_id.as_ref())
            && export["destinationcurrencyid"].as_str() == Some(currency)
            && export["hashtransfers"].is_string()
            && export["hashtransfers"] == import["hashtransfers"],
        "Export/import batch binding mismatch"
    );
    let transfers = record["transfers"]
        .as_array()
        .context("Ordered transfer list absent")?;
    ensure!(
        export["numinputs"].as_u64() == Some(transfers.len() as u64),
        "Transfer count mismatch"
    );
    trail.edges.push(LifecycleEdge {
        source: export_ref.clone(),
        target: root.clone(),
        relation: "export-import".into(),
        resolution: "structural".into(),
        detail: "Exact export outpoint, systems, currency and batch hash match".into(),
    });
    let (start, region) =
        generated_region(tx, import, root.vout).context("Generated-output region unavailable")?;
    trail.notes.push(format!(
        "Observed import at height {height}: {} ordered transfer(s); {} generated output(s).",
        transfers.len(),
        region.len()
    ));
    if launch_funding::applicable(record) {
        if export_tx.source_id == fresh.source_id
            && let Some(verified) = (launch_funding::Evidence {
                source: &fresh.source_id,
                root,
                record,
                import_tx: tx,
                export_tx: &export_tx.result,
                export_vout,
            })
            .verify(&client)
            .await
        {
            verified.links(trail, root);
            for receipt in &verified.receipts {
                trail.entries.push(entry(&receipt.funding, "Launch contribution",
                    "Exact reserve-transfer outpoint consumed in the source export's ordered input range"));
                trail.edges.push(LifecycleEdge {
                    source: receipt.funding.clone(),
                    target: export_ref.clone(),
                    relation: "spent-by-export".into(),
                    resolution: "structural".into(),
                    detail: "Export spends this launch contribution at its matched batch position"
                        .into(),
                });
                verified.add_receipt(trail, receipt);
            }
            if let Some(rewards) = &verified.rewards {
                rewards.append(trail, root);
            }
            trail.notes.push("The launch phase links consume both the preceding import and its state. Service and reserve-maintenance outputs are separate from the verified customer receipts.".into());
            return Ok(());
        }
        trail.notes.push("The launch receipts could not be verified from the available definition, export inputs, and linked launch phases.".into());
    }
    if import_rewards::applicable(record) {
        if let Some(rewards) = (import_rewards::Evidence {
            source: &source_client,
            source_id: &export_tx.source_id,
            destination: &client,
            destination_id: &fresh.source_id,
            root,
            record,
            import_tx: tx,
            export_tx: &export_tx.result,
        })
        .verify()
        .await
        {
            trail.edges.push(LifecycleEdge {
                source: rewards.funding.clone(),
                target: export_ref,
                relation: "spent-by-export".into(),
                resolution: "structural".into(),
                detail: "Export consumes the exact verified funding outpoint".into(),
            });
            rewards.append(trail, root, true);
            return Ok(());
        }
        trail.notes.push("Submission rewards remain unattributed for this import. Their calculation requires the original export and definition, the producer block, and the active entropy header.".into());
    }
    if export_tx.source_id == fresh.source_id
        && import_rewards::exporters::settled_applicable(record, root.system_id.as_ref())
        && let Some(verified) = (import_rewards::exporters::SettledEvidence {
            source: &fresh.source_id,
            root,
            record,
            import_tx: tx,
            export_tx: &export_tx.result,
            export_vout,
        })
        .verify(&client)
        .await
    {
        for (funding, receipt) in verified.funding {
            trail.entries.push(entry(
                &funding,
                "Import funding",
                "Matched ordered reserve transfer",
            ));
            trail.entries.push(entry(
                &receipt,
                "Verified customer receipt",
                "Exact amount and recipient script from ordered conversion replay",
            ));
            trail.edges.push(LifecycleEdge {
                source: funding,
                target: receipt,
                relation: "import-customer-receipt".into(),
                resolution: "attributed".into(),
                detail: "Exact ordered reserve conversion".into(),
            });
        }
        verified.rewards.append(trail, root);
        return Ok(());
    }
    let allocations = if transfers.is_empty() {
        let definition = client.get_currency(currency).await?;
        ensure!(
            definition.source_id == fresh.source_id,
            "Definition source changed"
        );
        replay::replay_launch_allocations(
            import,
            &record["importnotarization"],
            export,
            &definition.result,
            transfers,
            root.system_id.as_ref(),
            region,
        )
    } else {
        None
    };
    let mut attributed = 0;
    if let Some(allocations) = allocations {
        for (offset, recipient, amount) in allocations {
            let target = OutpointRef {
                system_id: root.system_id.clone(),
                txid: root.txid.clone(),
                vout: u32::try_from(start + offset)?,
            };
            let detail = format!(
                "{amount} satoshis of {currency} to {recipient}; exact ordered definition allocation"
            );
            trail
                .entries
                .push(entry(&target, "Verified launch preallocation", &detail));
            trail.edges.push(LifecycleEdge {
                source: root.clone(),
                target,
                relation: "launch-preallocation".into(),
                resolution: "attributed".into(),
                detail,
            });
            attributed += 1;
        }
    }
    for offset in attributed..region.len() {
        let target = OutpointRef {
            system_id: root.system_id.clone(),
            txid: root.txid.clone(),
            vout: u32::try_from(start + offset)?,
        };
        trail.entries.push(entry(&target,"Generated output (unattributed)",
            "Inside the import's generated region; fee, reward and customer roles require separate evidence"));
    }
    trail.notes.push("Reserve-maintenance outputs outside this generated region are excluded. Public amounts do not establish private recipients or individual service-reward roles.".into());
    Ok(())
}
