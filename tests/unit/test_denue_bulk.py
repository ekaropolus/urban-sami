from __future__ import annotations

from pathlib import Path

from urban_sami.io.denue_bulk import iter_denue_bulk_rows


def test_iter_denue_bulk_rows_normalizes_selected_columns(tmp_path: Path):
    csv_path = tmp_path / "denue.csv"
    csv_path.write_text(
        "\n".join(
            [
                "id,codigo_act,per_ocu,cve_ent,entidad,cve_mun,municipio,ageb,manzana,latitud,longitud",
                "1,461110,0 a 5 personas,09,Ciudad de Mexico,003,Coyoacan,1234,001,19.35,-99.16",
            ]
        ),
        encoding="latin-1",
    )
    rows = iter_denue_bulk_rows(csv_path)
    assert len(rows) == 1
    assert rows[0].denue_id == "1"
    assert rows[0].state_code == "09"
    assert rows[0].city_code == "09003"
    assert rows[0].scian_code == "461110"
    assert rows[0].latitude == 19.35

