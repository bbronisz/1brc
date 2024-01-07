internal static class Consts
{
    public const int OneThreadBlockSize = 0x30000000;//0x8000000; //128 MB;//0x600000;//0x10000000;// 256 megabytes
    public const int PrevAccessorLength = 100;
    public const int NewLineLength = 1;
}

internal static class Exts
{
    private const byte LF = (byte)'\n';

    public static int GetNewLineIndex(this ReadOnlySpan<byte> bytes) =>
        bytes.IndexOf(LF);

    public static int GetNewLineIndex(this Span<byte> bytes) =>
        bytes.IndexOf(LF);
}