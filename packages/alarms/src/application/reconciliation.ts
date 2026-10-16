import type { ProviderEvidenceEntry } from '../domain/delivery-evidence';

export interface ReconciliationClaim {
  readonly alarmId: string;
  readonly deliveryId: string;
  readonly recipientId: string;
  readonly clientRef: string;
  readonly sourceState: 'UNKNOWN' | 'NEEDS_REVIEW';
  readonly reconcileGeneration: number;
  readonly reconcileToken: string;
}

export type HistoryLookupResult =
  | { readonly kind: 'OBSERVATION'; readonly entries: readonly ProviderEvidenceEntry[] }
  | { readonly kind: 'UNAVAILABLE'; readonly code: string };

export interface ProviderMessageHistory {
  lookup(clientRef: string, signal?: AbortSignal): Promise<HistoryLookupResult>;
}

export interface ReconciliationRepository {
  claimNext(leaseDurationMs: number): Promise<ReconciliationClaim | null>;
  record(claim: ReconciliationClaim, result: HistoryLookupResult): Promise<void>;
}

export interface ReconciliationProcessorOptions {
  readonly leaseDurationMs?: number;
}

export class ReconciliationProcessor {
  private readonly leaseDurationMs: number;

  public constructor(
    private readonly repository: ReconciliationRepository,
    private readonly history: ProviderMessageHistory,
    options: ReconciliationProcessorOptions = {},
  ) {
    this.leaseDurationMs = options.leaseDurationMs ?? 15_000;
  }

  public async runOnce(signal?: AbortSignal): Promise<boolean> {
    const claim = await this.repository.claimNext(this.leaseDurationMs);
    if (claim === null) {
      return false;
    }

    let result: HistoryLookupResult;
    try {
      result = await this.history.lookup(claim.clientRef, signal);
    } catch (error) {
      result = {
        kind: 'UNAVAILABLE',
        code: error instanceof Error ? error.name : 'HISTORY_LOOKUP_ERROR',
      };
    }
    await this.repository.record(claim, result);
    return true;
  }
}
