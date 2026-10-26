import { describe, expect, it } from "vitest";
import { parseLive, positionMarkers, snapshotMarkers } from "../lib/live";

// Shapes as CTA returns them (every value a string), relayed unmodified by live-api/api/live.ts.
function train(over: Record<string, unknown> = {}) {
  return {
    rn: "808", destSt: "30173", destNm: "Howard", trDr: "1", nextStaId: "40900", nextStpId: "30173",
    nextStaNm: "Howard", prdt: "2026-10-26T14:43:40", arrT: "2026-10-26T14:44:40", isApp: "1", isDly: "0",
    flags: null, lat: "42.01588", lon: "-87.66909", heading: "310", ...over,
  };
}

function eta(over: Record<string, unknown> = {}) {
  return {
    staId: "41320", stpId: "30257", staNm: "Belmont", stpDe: "Service toward Howard", rn: "808", rt: "Red",
    destSt: "30173", destNm: "Howard", trDr: "1", prdt: "2026-10-26T14:43:40", arrT: "2026-10-26T14:50:10",
    isApp: "0", isSch: "0", isDly: "0", isFlt: "0", flags: null, lat: "41.93", lon: "-87.65", heading: "358", ...over,
  };
}

function response(over: Record<string, unknown> = {}) {
  return {
    fetched_at_ms: 1793044218358,
    positions: { tmst: "2026-10-26T14:45:00", errCd: "0", route: [{ "@name": "red", train: [train()] }] },
    arrivals: [{ tmst: "2026-10-26T14:45:10", errCd: "0", eta: [eta()] }],
    ...over,
  };
}

describe("live feed parsing", () => {
  it("maps Locations route names to dashboard route codes and types every field", () => {
    const { positions } = parseLive(response({
      positions: { route: [{ "@name": "brn", train: [train({ isApp: "0", isDly: "1" })] }] },
    }));
    expect(positions).toEqual([{
      route_code: "Brn", run_number: "808", destination_name: "Howard", next_station_name: "Howard",
      next_arrival_chicago: "2026-10-26T14:44:40", is_approaching: false, is_delayed: true,
      latitude: 42.01588, longitude: -87.66909, heading: 310,
    }]);
  });

  it("handles a route with no trains, a single train as a bare object, and unknown routes", () => {
    const { positions } = parseLive(response({
      positions: { route: [{ "@name": "y" }, { "@name": "g", train: train({ rn: "001" }) }, { "@name": "zz", train: [train()] }] },
    }));
    expect(positions.map((p) => [p.route_code, p.run_number])).toEqual([["G", "001"]]);
  });

  it("drops trains without a position (blank or 0,0 coordinates)", () => {
    const { positions } = parseLive(response({
      positions: { route: [{ "@name": "red", train: [train({ lat: "0", lon: "0" }), train({ lat: "", lon: "" }), train({ rn: "2" })] }] },
    }));
    expect(positions.map((p) => p.run_number)).toEqual(["2"]);
  });

  it("parses arrivals into snapshot predictions, timed from CTA's response time", () => {
    const [p] = parseLive(response()).predictions;
    expect(p).toMatchObject({
      station_id: "41320", station_name: "Belmont", stop_id: "30257", route_code: "Red", run_number: "808",
      predicted_arrival_chicago: "2026-10-26T14:50:10", seconds_until_arrival: 300, is_scheduled: false,
      latitude: 41.93, longitude: -87.65, observed_at_ms: 1793044218358,
    });
  });

  it("keeps schedule-based predictions without coordinates, and skips malformed ones", () => {
    const { predictions } = parseLive(response({
      arrivals: [{ tmst: "2026-10-26T14:45:10", eta: [eta({ isSch: "1", lat: "0", lon: "0" }), eta({ rn: "" }), eta({ rt: "Pexp" })] }],
    }));
    expect(predictions).toHaveLength(1);
    expect(predictions[0]).toMatchObject({ is_scheduled: true, latitude: null, longitude: null });
  });

  it("merges arrival batches and orders them by station, then arrival", () => {
    const { predictions } = parseLive(response({
      arrivals: [
        { tmst: "2026-10-26T14:45:10", eta: [eta({ staId: "41320", arrT: "2026-10-26T14:52:00" }), eta({ staId: "41320", arrT: "2026-10-26T14:47:00" })] },
        { tmst: "2026-10-26T14:45:10", eta: eta({ staId: "40380", arrT: "2026-10-26T14:59:00" }) },
        { tmst: "2026-10-26T14:45:10" }, // a station with no trains due
      ],
    }));
    expect(predictions.map((p) => [p.station_id, p.predicted_arrival_chicago.slice(11, 16)])).toEqual([
      ["40380", "14:59"], ["41320", "14:47"], ["41320", "14:52"],
    ]);
  });

  it("rejects a response that isn't from the live feed", () => {
    expect(() => parseLive({ error: "CTA error 102" })).toThrow();
    expect(() => parseLive(null)).toThrow();
  });
});

describe("map markers", () => {
  it("describes live trains by their next stop", () => {
    const [m] = positionMarkers(parseLive(response()).positions);
    expect(m).toMatchObject({ route_code: "Red", latitude: 42.01588, longitude: -87.66909 });
    expect(m.title).toBe("Red Line run 808 to Howard");
    expect(m.detail).toBe("Approaching Howard 2:44 PM");
  });

  it("skips snapshot predictions without coordinates", () => {
    const { predictions } = parseLive(response({
      arrivals: [{ tmst: "2026-10-26T14:45:10", eta: [eta({ isDly: "1" }), eta({ lat: "", lon: "" })] }],
    }));
    const markers = snapshotMarkers(predictions);
    expect(markers).toHaveLength(1);
    expect(markers[0].detail).toBe("Predicted at Belmont 2:50 PM · delayed");
  });
});
