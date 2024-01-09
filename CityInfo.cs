// See https://aka.ms/new-console-template for more information
internal class CityInfo
{
    public CityInfo(string city, double value)
    {
        City = city;
        //HashCode = hashCode;
        Min = Max = Sum = value;
        Count = 1;
    }

    //public int HashCode { get; }
    public string City { get; }
    public double Min { get; private set; }
    public double Max { get; private set; }
    public double Sum { get; private set; }
    public int Count { get; private set; }

    public void Add(double value)
    {
        if (Min > value) Min = value;
        else if (Max < value) Max = value;
        Sum += value;
        Count++;
    }

    public void Merge(CityInfo other)
    {
        Min = double.Min(Min, other.Min);
        Max = double.Max(Max, other.Max);
        Sum += other.Sum;
        Count += other.Count;
    }

    public void PrintResult(string prev)
    {
        Console.Write("{4}{0}={1:0.0}/{2:0.0}/{3:0.0}", City, Min, Sum / Count, Max, prev);
    }
}

