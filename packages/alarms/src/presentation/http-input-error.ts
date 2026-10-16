export class HttpInputError extends Error {
  public constructor(
    public readonly status: 400 | 415 | 422,
    public readonly code: string,
    message: string,
    public readonly errors?: readonly object[],
  ) {
    super(message);
    this.name = 'HttpInputError';
  }
}
