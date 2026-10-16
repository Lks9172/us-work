export interface ProviderEvidenceEntry {
  readonly messageId: string;
  readonly recipientId: string;
  readonly sentAt?: string;
}

export type EvidenceDecision =
  | { readonly kind: 'RECIPIENT_MISMATCH' }
  | { readonly kind: 'EVIDENCE_CONFLICT' }
  | {
      readonly kind: 'MATCH';
      readonly distinctMessageIds: readonly string[];
      readonly providerSentAt: string | null;
    };

export function evaluateEvidence(
  entries: readonly ProviderEvidenceEntry[],
  expectedRecipientId: string,
): EvidenceDecision {
  if (entries.some(({ recipientId }) => recipientId !== expectedRecipientId)) {
    return { kind: 'RECIPIENT_MISMATCH' };
  }

  const byMessageId = new Map<string, ProviderEvidenceEntry>();
  for (const entry of entries) {
    const current = byMessageId.get(entry.messageId);
    if (
      current?.sentAt !== undefined &&
      entry.sentAt !== undefined &&
      current.sentAt !== entry.sentAt
    ) {
      return { kind: 'EVIDENCE_CONFLICT' };
    }
    byMessageId.set(
      entry.messageId,
      current === undefined || (current.sentAt === undefined && entry.sentAt !== undefined)
        ? entry
        : current,
    );
  }

  const distinctMessageIds = [...byMessageId.keys()].sort();
  const sentTimes = [...byMessageId.values()]
    .map(({ sentAt }) => sentAt)
    .filter((value): value is string => value !== undefined)
    .sort();

  return {
    kind: 'MATCH',
    distinctMessageIds,
    providerSentAt: sentTimes[0] ?? null,
  };
}
