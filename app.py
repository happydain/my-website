"""
Chinook Analytics Dashboard
음악 스토어 경영분석 대시보드 (Streamlit + SQLite)

실행 방법:
    pip install -r requirements.txt
    streamlit run app.py
"""

import sqlite3
import os
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# ============================================================
# 페이지 기본 설정
# ============================================================
st.set_page_config(
    page_title="Chinook Analytics",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded",
)

PLOTLY_FONT = dict(
    family="Noto Sans KR, Malgun Gothic, AppleGothic, sans-serif",
    size=12,
)

COLOR_PALETTE = [
    "#2563eb", "#7c3aed", "#f59e0b", "#10b981", "#ef4444",
    "#06b6d4", "#ec4899", "#84cc16", "#f97316", "#6366f1"
]

DB_PATH = "chinook.db"


# ============================================================
# 공통 DB 유틸
# ============================================================
def get_connection():
    """SQLite 연결 생성"""
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def get_table_name(candidates):
    """후보 테이블명 중 실제 존재하는 이름 반환"""
    if not os.path.exists(DB_PATH):
        return None

    conn = get_connection()
    try:
        existing = pd.read_sql(
            "SELECT name FROM sqlite_master WHERE type='table'",
            conn
        )["name"].tolist()
        existing_lower = {name.lower(): name for name in existing}

        for candidate in candidates:
            if candidate.lower() in existing_lower:
                return existing_lower[candidate.lower()]
        return None
    finally:
        conn.close()


# ============================================================
# 데이터 로딩 (캐싱)
# ============================================================
@st.cache_data(show_spinner=False)
def load_data():
    """DB에서 필요한 데이터를 로드"""
    if not os.path.exists(DB_PATH):
        return None

    invoice_table = get_table_name(["invoices", "Invoice"])
    customer_table = get_table_name(["customers", "Customer"])
    employee_table = get_table_name(["employees", "Employee"])
    invoice_line_table = get_table_name(["invoice_items", "InvoiceLine"])
    track_table = get_table_name(["tracks", "Track"])
    genre_table = get_table_name(["genres", "Genre"])
    album_table = get_table_name(["albums", "Album"])
    artist_table = get_table_name(["artists", "Artist"])

    if not all([
        invoice_table, customer_table, employee_table, invoice_line_table,
        track_table, genre_table, album_table, artist_table
    ]):
        return None

    conn = get_connection()
    try:
        invoices_query = f"""
            SELECT
                i.InvoiceId,
                i.CustomerId,
                i.InvoiceDate,
                i.BillingCountry AS Country,
                i.BillingCity AS City,
                i.Total,
                c.FirstName || ' ' || c.LastName AS CustomerName,
                c.SupportRepId,
                e.FirstName || ' ' || e.LastName AS SalesRep
            FROM {invoice_table} i
            LEFT JOIN {customer_table} c ON i.CustomerId = c.CustomerId
            LEFT JOIN {employee_table} e ON c.SupportRepId = e.EmployeeId
        """
        df_invoices = pd.read_sql(invoices_query, conn)
        df_invoices["InvoiceDate"] = pd.to_datetime(df_invoices["InvoiceDate"], errors="coerce")
        df_invoices["Year"] = df_invoices["InvoiceDate"].dt.year
        df_invoices["Month"] = df_invoices["InvoiceDate"].dt.month
        df_invoices["YearMonth"] = df_invoices["InvoiceDate"].dt.to_period("M").astype(str)

        items_query = f"""
            SELECT
                ii.InvoiceLineId,
                ii.InvoiceId,
                ii.TrackId,
                ii.UnitPrice,
                ii.Quantity,
                (ii.UnitPrice * ii.Quantity) AS LineTotal,
                t.Name AS TrackName,
                t.GenreId,
                g.Name AS Genre,
                t.AlbumId,
                al.Title AS Album,
                al.ArtistId,
                ar.Name AS Artist,
                i.InvoiceDate,
                i.BillingCountry AS Country
            FROM {invoice_line_table} ii
            LEFT JOIN {track_table} t ON ii.TrackId = t.TrackId
            LEFT JOIN {genre_table} g ON t.GenreId = g.GenreId
            LEFT JOIN {album_table} al ON t.AlbumId = al.AlbumId
            LEFT JOIN {artist_table} ar ON al.ArtistId = ar.ArtistId
            LEFT JOIN {invoice_table} i ON ii.InvoiceId = i.InvoiceId
        """
        df_items = pd.read_sql(items_query, conn)
        df_items["InvoiceDate"] = pd.to_datetime(df_items["InvoiceDate"], errors="coerce")
        df_items["Year"] = df_items["InvoiceDate"].dt.year

        return {"invoices": df_invoices, "items": df_items}
    finally:
        conn.close()


# ============================================================
# 고객 관리용 함수
# ============================================================
def get_customers():
    """customers 테이블 조회"""
    customer_table = get_table_name(["customers", "Customer"])
    if not customer_table:
        return pd.DataFrame()

    conn = get_connection()
    try:
        query = f"""
            SELECT
                CustomerId,
                FirstName,
                LastName,
                Company,
                Address,
                City,
                State,
                Country,
                PostalCode,
                Phone,
                Fax,
                Email,
                SupportRepId
            FROM {customer_table}
            ORDER BY CustomerId
        """
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def get_first_support_rep_id():
    """기본 SupportRepId 조회"""
    employee_table = get_table_name(["employees", "Employee"])
    if not employee_table:
        return None

    conn = get_connection()
    try:
        query = f"SELECT EmployeeId FROM {employee_table} ORDER BY EmployeeId LIMIT 1"
        row = conn.execute(query).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def update_customer(customer_id, first_name, last_name, company, city, country, email):
    """고객 정보 수정"""
    customer_table = get_table_name(["customers", "Customer"])
    if not customer_table:
        raise ValueError("customers 테이블을 찾을 수 없습니다.")

    conn = get_connection()
    try:
        cursor = conn.cursor()
        query = f"""
            UPDATE {customer_table}
            SET
                FirstName = ?,
                LastName = ?,
                Company = ?,
                City = ?,
                Country = ?,
                Email = ?
            WHERE CustomerId = ?
        """
        cursor.execute(
            query,
            (first_name, last_name, company, city, country, email, customer_id)
        )
        conn.commit()
        return cursor.rowcount
    finally:
        conn.close()


def insert_customer(first_name, last_name, company, city, country, email):
    """신규 고객 추가"""
    customer_table = get_table_name(["customers", "Customer"])
    if not customer_table:
        raise ValueError("customers 테이블을 찾을 수 없습니다.")

    support_rep_id = get_first_support_rep_id()

    conn = get_connection()
    try:
        cursor = conn.cursor()
        query = f"""
            INSERT INTO {customer_table} (
                FirstName, LastName, Company, City, Country, Email, SupportRepId
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(
            query,
            (
                first_name.strip(),
                last_name.strip(),
                company.strip() if company else None,
                city.strip() if city else None,
                country.strip() if country else None,
                email.strip(),
                support_rep_id
            )
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


# ============================================================
# 유틸리티 함수
# ============================================================
def apply_filters(df, year_range, countries):
    """공통 필터 적용"""
    mask = (df["Year"] >= year_range[0]) & (df["Year"] <= year_range[1])
    if countries:
        mask &= df["Country"].isin(countries)
    return df[mask].copy()


def style_plotly(fig, height=400):
    """Plotly 차트 공통 스타일"""
    fig.update_layout(
        font=PLOTLY_FONT,
        height=height,
        margin=dict(l=20, r=20, t=50, b=20),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        hoverlabel=dict(font_family=PLOTLY_FONT["family"]),
    )
    return fig


def format_currency(value):
    try:
        return f"${value:,.2f}"
    except Exception:
        return "$0.00"


def safe_percent(numerator, denominator):
    if denominator == 0:
        return 0
    return (numerator / denominator) * 100


def render_insight_box(title, insights, box_type="info"):
    """인사이트 박스 출력"""
    text = f"**{title}**\n\n"
    for idx, insight in enumerate(insights, 1):
        text += f"{idx}. {insight}\n\n"

    if box_type == "success":
        st.success(text)
    elif box_type == "warning":
        st.warning(text)
    else:
        st.info(text)


# ============================================================
# 페이지 1: 매출 Overview
# ============================================================
def page_overview(df_inv, df_inv_full):
    st.title("📊 매출 Overview")
    st.caption("전체 매출 추이와 핵심 지표를 한눈에 확인합니다.")

    if df_inv.empty:
        st.warning("선택한 조건에 해당하는 데이터가 없습니다. 사이드바 필터를 조정해주세요.")
        return

    total_revenue = df_inv["Total"].sum()
    total_orders = len(df_inv)
    total_customers = df_inv["CustomerId"].nunique()
    avg_order = total_revenue / total_orders if total_orders > 0 else 0

    full_revenue = df_inv_full["Total"].sum()
    full_orders = len(df_inv_full)
    full_customers = df_inv_full["CustomerId"].nunique()
    full_avg = full_revenue / full_orders if full_orders > 0 else 0

    delta_revenue = total_revenue - full_revenue
    delta_orders = total_orders - full_orders
    delta_customers = total_customers - full_customers
    delta_avg = avg_order - full_avg

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "총 매출",
            format_currency(total_revenue),
            delta=f"{delta_revenue:+,.2f}" if delta_revenue != 0 else None,
            delta_color="normal",
        )
    with col2:
        st.metric(
            "총 주문수",
            f"{total_orders:,}",
            delta=f"{delta_orders:+,}" if delta_orders != 0 else None,
        )
    with col3:
        st.metric(
            "고객수",
            f"{total_customers:,}",
            delta=f"{delta_customers:+,}" if delta_customers != 0 else None,
        )
    with col4:
        st.metric(
            "평균 주문액",
            format_currency(avg_order),
            delta=f"{delta_avg:+,.2f}" if delta_avg != 0 else None,
        )

    st.markdown("---")

    st.subheader("📈 연도별 매출 추이")
    yearly = df_inv.groupby("Year").agg(
        Revenue=("Total", "sum"),
        Orders=("InvoiceId", "count"),
    ).reset_index()

    fig_line = go.Figure()
    fig_line.add_trace(go.Scatter(
        x=yearly["Year"], y=yearly["Revenue"],
        mode="lines+markers+text",
        name="매출",
        line=dict(color=COLOR_PALETTE[0], width=3),
        marker=dict(size=10),
        text=[format_currency(v) for v in yearly["Revenue"]],
        textposition="top center",
        hovertemplate="<b>%{x}</b><br>매출: $%{y:,.2f}<extra></extra>",
    ))
    fig_line.update_layout(
        title="연도별 매출",
        xaxis_title="연도",
        yaxis_title="매출 ($)",
        xaxis=dict(dtick=1),
    )
    st.plotly_chart(style_plotly(fig_line, height=380), use_container_width=True)

    st.subheader("🔥 월별 매출 히트맵")
    heatmap = df_inv.groupby(["Year", "Month"])["Total"].sum().reset_index()
    pivot = heatmap.pivot(index="Year", columns="Month", values="Total").fillna(0)

    for m in range(1, 13):
        if m not in pivot.columns:
            pivot[m] = 0
    pivot = pivot[sorted(pivot.columns)]

    month_labels = [f"{m}월" for m in pivot.columns]
    fig_heat = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=month_labels,
        y=pivot.index,
        colorscale="Blues",
        text=[[f"${v:.0f}" if v > 0 else "" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        textfont={"size": 10},
        hovertemplate="<b>%{y}년 %{x}</b><br>매출: $%{z:,.2f}<extra></extra>",
        colorbar=dict(title="매출 ($)"),
    ))
    fig_heat.update_layout(
        title="연도 × 월 매출 히트맵",
        xaxis_title="월",
        yaxis_title="연도",
        yaxis=dict(dtick=1),
    )
    st.plotly_chart(style_plotly(fig_heat, height=350), use_container_width=True)

    st.markdown("---")
    yearly_sorted = yearly.sort_values("Revenue", ascending=False)
    best_year = int(yearly_sorted.iloc[0]["Year"])
    best_year_revenue = yearly_sorted.iloc[0]["Revenue"]

    month_rev = df_inv.groupby("Month")["Total"].sum().reset_index().sort_values("Total", ascending=False)
    best_month = int(month_rev.iloc[0]["Month"])
    best_month_revenue = month_rev.iloc[0]["Total"]

    insights = [
        f"선택한 기간의 총매출은 {format_currency(total_revenue)}이며, 총 {total_orders:,}건의 주문이 발생해 평균 주문액은 {format_currency(avg_order)}입니다.",
        f"가장 높은 연간 매출을 기록한 시점은 {best_year}년이며, 해당 연도 매출은 {format_currency(best_year_revenue)}입니다.",
        f"월 기준으로는 {best_month}월 매출이 가장 높았고, 누적 매출은 {format_currency(best_month_revenue)}입니다."
    ]
    render_insight_box("비즈니스 인사이트", insights, box_type="info")


# ============================================================
# 페이지 2: 고객 & 지역 분석
# ============================================================
def page_customers(df_inv):
    st.title("🌍 고객 & 지역 분석")
    st.caption("국가별 매출과 고객별 구매 패턴을 분석합니다.")

    if df_inv.empty:
        st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
        return

    st.subheader("🏆 국가별 매출 Top 10")
    country_rev = df_inv.groupby("Country").agg(
        Revenue=("Total", "sum"),
        Orders=("InvoiceId", "count"),
        Customers=("CustomerId", "nunique"),
    ).reset_index().sort_values("Revenue", ascending=False).head(10)

    fig_country = px.bar(
        country_rev.sort_values("Revenue"),
        x="Revenue", y="Country",
        orientation="h",
        text=country_rev.sort_values("Revenue")["Revenue"].apply(lambda v: f"${v:,.0f}"),
        color="Revenue",
        color_continuous_scale="Blues",
    )
    fig_country.update_traces(textposition="outside")
    fig_country.update_layout(
        xaxis_title="매출 ($)",
        yaxis_title="",
        coloraxis_showscale=False,
    )
    st.plotly_chart(style_plotly(fig_country, height=420), use_container_width=True)

    st.subheader("💎 국가별 고객 수 vs 평균 주문액")
    scatter = df_inv.groupby("Country").agg(
        Customers=("CustomerId", "nunique"),
        AvgOrder=("Total", "mean"),
        TotalRevenue=("Total", "sum"),
    ).reset_index()

    fig_scatter = px.scatter(
        scatter, x="Customers", y="AvgOrder",
        size="TotalRevenue", color="TotalRevenue",
        hover_name="Country",
        text="Country",
        color_continuous_scale="Viridis",
        size_max=50,
        labels={
            "Customers": "고객 수",
            "AvgOrder": "평균 주문액 ($)",
            "TotalRevenue": "총 매출 ($)",
        },
    )
    fig_scatter.update_traces(textposition="top center", textfont_size=10)
    st.plotly_chart(style_plotly(fig_scatter, height=450), use_container_width=True)

    st.subheader("👤 고객별 구매 순위")
    customer_rank = df_inv.groupby(["CustomerId", "CustomerName", "Country"]).agg(
        총주문수=("InvoiceId", "count"),
        총구매액=("Total", "sum"),
        평균주문액=("Total", "mean"),
    ).reset_index().sort_values("총구매액", ascending=False)

    customer_rank["총구매액"] = customer_rank["총구매액"].round(2)
    customer_rank["평균주문액"] = customer_rank["평균주문액"].round(2)
    customer_rank = customer_rank.rename(columns={
        "CustomerName": "고객명",
        "Country": "국가",
    })[["고객명", "국가", "총주문수", "총구매액", "평균주문액"]]

    search = st.text_input("🔍 고객명 또는 국가로 검색", placeholder="예: Smith, Germany...")
    if search:
        mask = (
            customer_rank["고객명"].str.contains(search, case=False, na=False)
            | customer_rank["국가"].str.contains(search, case=False, na=False)
        )
        customer_rank = customer_rank[mask]

    st.dataframe(
        customer_rank,
        use_container_width=True,
        height=400,
        column_config={
            "총구매액": st.column_config.NumberColumn(format="$%.2f"),
            "평균주문액": st.column_config.NumberColumn(format="$%.2f"),
        },
        hide_index=True,
    )
    st.caption(f"총 {len(customer_rank)}명의 고객")

    # 추가 차트 1: 도시별 매출 Top 10
    st.markdown("---")
    st.subheader("🏙️ 도시별 매출 Top 10")
    city_rev = (
        df_inv[df_inv["City"].notna()]
        .groupby("City")["Total"]
        .sum()
        .reset_index()
        .sort_values("Total", ascending=False)
        .head(10)
    )

    fig_city = px.bar(
        city_rev.sort_values("Total"),
        x="Total",
        y="City",
        orientation="h",
        text=city_rev.sort_values("Total")["Total"].apply(lambda v: f"${v:,.0f}"),
        color="Total",
        color_continuous_scale="Tealgrn",
    )
    fig_city.update_traces(textposition="outside")
    fig_city.update_layout(
        xaxis_title="매출 ($)",
        yaxis_title="도시",
        coloraxis_showscale=False,
    )
    st.plotly_chart(style_plotly(fig_city, height=420), use_container_width=True)

    # 추가 차트 2: 고객당 평균 매출 Top 국가
    st.subheader("💰 고객당 평균 매출 Top 국가")
    country_eff = df_inv.groupby("Country").agg(
        Revenue=("Total", "sum"),
        Customers=("CustomerId", "nunique"),
    ).reset_index()
    country_eff = country_eff[country_eff["Customers"] > 0].copy()
    country_eff["RevenuePerCustomer"] = country_eff["Revenue"] / country_eff["Customers"]
    country_eff = country_eff.sort_values("RevenuePerCustomer", ascending=False).head(10)

    fig_eff = px.bar(
        country_eff.sort_values("RevenuePerCustomer"),
        x="RevenuePerCustomer",
        y="Country",
        orientation="h",
        text=country_eff.sort_values("RevenuePerCustomer")["RevenuePerCustomer"].apply(lambda v: f"${v:,.2f}"),
        color="RevenuePerCustomer",
        color_continuous_scale="Sunset",
    )
    fig_eff.update_traces(textposition="outside")
    fig_eff.update_layout(
        xaxis_title="고객당 평균 매출 ($)",
        yaxis_title="국가",
        coloraxis_showscale=False,
    )
    st.plotly_chart(style_plotly(fig_eff, height=420), use_container_width=True)

    # 추가 차트 3: 고객별 누적 구매액 분포
    st.subheader("👥 고객별 누적 구매액 분포")
    customer_sales = df_inv.groupby("CustomerId").agg(
        CustomerName=("CustomerName", "first"),
        TotalSpent=("Total", "sum")
    ).reset_index()

    fig_hist = px.histogram(
        customer_sales,
        x="TotalSpent",
        nbins=15,
        labels={"TotalSpent": "고객별 누적 구매액 ($)"},
    )
    fig_hist.update_layout(
        xaxis_title="고객별 누적 구매액 ($)",
        yaxis_title="고객 수",
    )
    st.plotly_chart(style_plotly(fig_hist, height=400), use_container_width=True)

    # 인사이트
    st.markdown("---")
    full_country = df_inv.groupby("Country")["Total"].sum().sort_values(ascending=False)
    top_country = full_country.index[0]
    top_country_revenue = full_country.iloc[0]
    top_country_share = safe_percent(top_country_revenue, full_country.sum())

    customer_contrib = df_inv.groupby("CustomerId")["Total"].sum().sort_values(ascending=False)
    top10_share = safe_percent(customer_contrib.head(10).sum(), customer_contrib.sum())

    highest_avg_order = scatter.sort_values("AvgOrder", ascending=False).iloc[0]

    top_city = city_rev.sort_values("Total", ascending=False).iloc[0]
    top_eff_country = country_eff.sort_values("RevenuePerCustomer", ascending=False).iloc[0]

    insights = [
        f"국가별 매출 1위는 {top_country}이며, 전체 매출의 {top_country_share:.1f}%를 차지합니다.",
        f"상위 10명의 고객이 전체 매출의 {top10_share:.1f}%를 기여해 핵심 고객 집중도가 존재합니다.",
        f"고객 수 대비 평균 주문액이 가장 높은 국가는 {highest_avg_order['Country']}이며, 평균 주문액은 {format_currency(highest_avg_order['AvgOrder'])}입니다.",
        f"도시 기준 매출 1위는 {top_city['City']}이며, 누적 매출은 {format_currency(top_city['Total'])}입니다.",
        f"고객당 평균 매출이 가장 높은 국가는 {top_eff_country['Country']}이며, 고객 1인당 평균 매출은 {format_currency(top_eff_country['RevenuePerCustomer'])}입니다."
    ]
    render_insight_box("비즈니스 인사이트", insights, box_type="success")


# ============================================================
# 페이지 3: 장르 & 상품 분석
# ============================================================
def page_genres(df_items):
    st.title("🎵 장르 & 상품 분석")
    st.caption("음악 장르별 판매 트렌드와 인기 아티스트를 분석합니다.")

    if df_items.empty:
        st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
        return

    col_a, col_b = st.columns([1, 1])

    with col_a:
        st.subheader("🍩 장르별 판매량 비중")
        genre_qty = df_items.groupby("Genre").agg(
            Quantity=("Quantity", "sum"),
            Revenue=("LineTotal", "sum"),
        ).reset_index().sort_values("Quantity", ascending=False)

        if len(genre_qty) > 8:
            top = genre_qty.head(8)
            others_qty = genre_qty.iloc[8:]["Quantity"].sum()
            others_rev = genre_qty.iloc[8:]["Revenue"].sum()
            top = pd.concat([top, pd.DataFrame([{
                "Genre": "기타", "Quantity": others_qty, "Revenue": others_rev
            }])], ignore_index=True)
        else:
            top = genre_qty

        fig_donut = go.Figure(data=[go.Pie(
            labels=top["Genre"],
            values=top["Quantity"],
            hole=0.5,
            marker=dict(colors=COLOR_PALETTE),
            textinfo="label+percent",
            hovertemplate="<b>%{label}</b><br>판매량: %{value}곡<br>비중: %{percent}<extra></extra>",
        )])
        fig_donut.update_layout(
            showlegend=True,
            legend=dict(orientation="v", x=1.0, y=0.5),
        )
        st.plotly_chart(style_plotly(fig_donut, height=400), use_container_width=True)

    with col_b:
        st.subheader("📊 장르별 매출 요약")
        genre_summary = df_items.groupby("Genre").agg(
            판매량=("Quantity", "sum"),
            매출=("LineTotal", "sum"),
        ).reset_index().sort_values("매출", ascending=False).head(10)
        genre_summary["매출"] = genre_summary["매출"].round(2)
        st.dataframe(
            genre_summary,
            use_container_width=True,
            height=400,
            column_config={
                "매출": st.column_config.NumberColumn(format="$%.2f"),
            },
            hide_index=True,
        )

    st.subheader("📈 장르별 매출 트렌드 (Top 6)")
    top_genres = df_items.groupby("Genre")["LineTotal"].sum().nlargest(6).index.tolist()
    trend = df_items[df_items["Genre"].isin(top_genres)].groupby(["Year", "Genre"])["LineTotal"].sum().reset_index()

    fig_area = px.area(
        trend, x="Year", y="LineTotal", color="Genre",
        color_discrete_sequence=COLOR_PALETTE,
        labels={"LineTotal": "매출 ($)", "Year": "연도"},
    )
    fig_area.update_layout(
        xaxis=dict(dtick=1),
        hovermode="x unified",
    )
    st.plotly_chart(style_plotly(fig_area, height=400), use_container_width=True)

    st.subheader("🎤 인기 아티스트 Top 15 (매출 기준)")
    artist_rev = df_items.groupby("Artist").agg(
        매출=("LineTotal", "sum"),
        판매량=("Quantity", "sum"),
    ).reset_index().sort_values("매출", ascending=False).head(15)

    fig_artist = px.bar(
        artist_rev.sort_values("매출"),
        x="매출", y="Artist",
        orientation="h",
        text=artist_rev.sort_values("매출")["매출"].apply(lambda v: f"${v:.2f}"),
        color="매출",
        color_continuous_scale="Purples",
    )
    fig_artist.update_traces(textposition="outside")
    fig_artist.update_layout(
        xaxis_title="매출 ($)",
        yaxis_title="",
        coloraxis_showscale=False,
    )
    st.plotly_chart(style_plotly(fig_artist, height=500), use_container_width=True)

    st.markdown("---")
    genre_revenue = df_items.groupby("Genre")["LineTotal"].sum().sort_values(ascending=False)
    top_genre = genre_revenue.index[0]
    top_genre_revenue = genre_revenue.iloc[0]
    top_genre_share = safe_percent(top_genre_revenue, genre_revenue.sum())

    artist_top = df_items.groupby("Artist")["LineTotal"].sum().sort_values(ascending=False)
    top_artist = artist_top.index[0]
    top_artist_revenue = artist_top.iloc[0]

    top6_revenue_share = safe_percent(
        df_items[df_items["Genre"].isin(top_genres)]["LineTotal"].sum(),
        df_items["LineTotal"].sum()
    )

    insights = [
        f"가장 높은 매출을 기록한 장르는 {top_genre}이며, 전체 장르 매출의 {top_genre_share:.1f}%를 차지합니다.",
        f"매출 기준 1위 아티스트는 {top_artist}이며, 누적 매출은 {format_currency(top_artist_revenue)}입니다.",
        f"상위 6개 장르가 전체 장르 매출의 {top6_revenue_share:.1f}%를 차지해 장르별 매출 집중 현상이 나타납니다."
    ]
    render_insight_box("비즈니스 인사이트", insights, box_type="info")


# ============================================================
# 페이지 4: 영업사원 성과
# ============================================================
def page_sales_rep(df_inv):
    st.title("👤 영업사원 성과")
    st.caption("Sales Support Agent별 성과를 비교 분석합니다.")

    if df_inv.empty:
        st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
        return

    df_rep = df_inv[df_inv["SalesRep"].notna()].copy()

    if df_rep.empty:
        st.warning("영업사원 정보가 있는 데이터가 없습니다.")
        return

    rep_summary = df_rep.groupby("SalesRep").agg(
        매출=("Total", "sum"),
        주문수=("InvoiceId", "count"),
        고객수=("CustomerId", "nunique"),
    ).reset_index().sort_values("매출", ascending=False)

    cols = st.columns(len(rep_summary))
    for col, row in zip(cols, rep_summary.itertuples()):
        with col:
            st.metric(
                row.SalesRep,
                format_currency(row.매출),
                delta=f"{row.주문수}건 / {row.고객수}명",
                delta_color="off",
            )

    st.markdown("---")

    st.subheader("📊 담당자별 성과 비교")
    fig_compare = go.Figure()
    fig_compare.add_trace(go.Bar(
        name="매출 ($)", x=rep_summary["SalesRep"], y=rep_summary["매출"],
        marker_color=COLOR_PALETTE[0],
        text=[f"${v:.0f}" for v in rep_summary["매출"]],
        textposition="outside",
        yaxis="y",
    ))
    fig_compare.add_trace(go.Bar(
        name="주문수", x=rep_summary["SalesRep"], y=rep_summary["주문수"],
        marker_color=COLOR_PALETTE[1],
        text=rep_summary["주문수"],
        textposition="outside",
        yaxis="y2",
    ))
    fig_compare.add_trace(go.Bar(
        name="고객수", x=rep_summary["SalesRep"], y=rep_summary["고객수"],
        marker_color=COLOR_PALETTE[2],
        text=rep_summary["고객수"],
        textposition="outside",
        yaxis="y2",
    ))
    fig_compare.update_layout(
        barmode="group",
        yaxis=dict(title="매출 ($)", side="left"),
        yaxis2=dict(title="건수 / 명", side="right", overlaying="y"),
        legend=dict(orientation="h", y=1.12, x=0.5, xanchor="center"),
    )
    st.plotly_chart(style_plotly(fig_compare, height=420), use_container_width=True)

    st.subheader("📈 담당자별 월별 매출 추이")
    monthly = df_rep.groupby(["YearMonth", "SalesRep"])["Total"].sum().reset_index()
    monthly = monthly.sort_values("YearMonth")

    fig_monthly = px.line(
        monthly, x="YearMonth", y="Total", color="SalesRep",
        markers=True,
        color_discrete_sequence=COLOR_PALETTE,
        labels={"Total": "매출 ($)", "YearMonth": "연-월", "SalesRep": "담당자"},
    )
    fig_monthly.update_layout(
        hovermode="x unified",
        xaxis=dict(tickangle=-45),
    )
    st.plotly_chart(style_plotly(fig_monthly, height=400), use_container_width=True)

    st.subheader("🌐 담당자별 고객 국가 분포")
    country_dist = df_rep.groupby(["SalesRep", "Country"]).agg(
        매출=("Total", "sum"),
        고객수=("CustomerId", "nunique"),
    ).reset_index()

    fig_dist = px.sunburst(
        country_dist,
        path=["SalesRep", "Country"],
        values="매출",
        color="매출",
        color_continuous_scale="Blues",
    )
    st.plotly_chart(style_plotly(fig_dist, height=500), use_container_width=True)

    st.markdown("---")
    top_rep = rep_summary.iloc[0]
    top_rep_share = safe_percent(top_rep["매출"], rep_summary["매출"].sum())

    best_customer_rep = rep_summary.sort_values("고객수", ascending=False).iloc[0]
    best_order_rep = rep_summary.sort_values("주문수", ascending=False).iloc[0]

    insights = [
        f"매출 기준 1위 영업사원은 {top_rep['SalesRep']}이며, 전체 담당자 매출의 {top_rep_share:.1f}%를 차지합니다.",
        f"고객 수를 가장 많이 관리하는 담당자는 {best_customer_rep['SalesRep']}이며, 총 {int(best_customer_rep['고객수'])}명의 고객을 담당합니다.",
        f"주문 건수 기준 최고 성과 담당자는 {best_order_rep['SalesRep']}이며, 총 {int(best_order_rep['주문수'])}건의 주문을 처리했습니다."
    ]
    render_insight_box("비즈니스 인사이트", insights, box_type="success")


# ============================================================
# 페이지 5: 고객 관리
# ============================================================
def page_customer_management():
    st.title("🧾 고객 관리")
    st.caption("기존 고객 정보 조회, 특정 고객 정보 수정, 신규 고객 추가가 가능합니다.")

    df_customers = get_customers()

    if df_customers.empty:
        st.warning("customers 테이블 데이터를 불러올 수 없습니다.")
        return

    tab1, tab2, tab3 = st.tabs(["📋 고객 조회", "✏️ 고객 수정", "➕ 신규 고객 추가"])

    with tab1:
        st.subheader("기존 고객 정보 확인")

        col1, col2 = st.columns(2)
        with col1:
            search_name = st.text_input("고객명 검색", placeholder="예: Maria, Smith")
        with col2:
            country_options = ["전체"] + sorted(df_customers["Country"].dropna().unique().tolist())
            search_country = st.selectbox("국가 검색", country_options)

        df_view = df_customers.copy()

        if search_name:
            mask = (
                df_view["FirstName"].fillna("").str.contains(search_name, case=False, na=False)
                | df_view["LastName"].fillna("").str.contains(search_name, case=False, na=False)
            )
            df_view = df_view[mask]

        if search_country != "전체":
            df_view = df_view[df_view["Country"] == search_country]

        show_columns = [
            "CustomerId", "FirstName", "LastName", "Company",
            "City", "Country", "Email", "Phone", "SupportRepId"
        ]

        st.dataframe(
            df_view[show_columns],
            use_container_width=True,
            height=450,
            hide_index=True,
        )
        st.caption(f"조회 결과: {len(df_view)}명")

    with tab2:
        st.subheader("특정 고객 정보 업데이트")

        customer_options = df_customers.apply(
            lambda row: f"{int(row['CustomerId'])} | {row['FirstName']} {row['LastName']} | {row['Country']}",
            axis=1
        ).tolist()

        selected_customer = st.selectbox("수정할 고객 선택", customer_options)

        selected_id = int(selected_customer.split("|")[0].strip())
        customer_row = df_customers[df_customers["CustomerId"] == selected_id].iloc[0]

        with st.form("update_customer_form"):
            first_name = st.text_input("FirstName", value="" if pd.isna(customer_row["FirstName"]) else str(customer_row["FirstName"]))
            last_name = st.text_input("LastName", value="" if pd.isna(customer_row["LastName"]) else str(customer_row["LastName"]))
            company = st.text_input("Company", value="" if pd.isna(customer_row["Company"]) else str(customer_row["Company"]))
            city = st.text_input("City", value="" if pd.isna(customer_row["City"]) else str(customer_row["City"]))
            country = st.text_input("Country", value="" if pd.isna(customer_row["Country"]) else str(customer_row["Country"]))
            email = st.text_input("Email", value="" if pd.isna(customer_row["Email"]) else str(customer_row["Email"]))

            submitted = st.form_submit_button("고객 정보 수정")

            if submitted:
                if not first_name.strip() or not last_name.strip() or not email.strip():
                    st.warning("FirstName, LastName, Email은 필수 입력값입니다.")
                else:
                    updated_rows = update_customer(
                        selected_id,
                        first_name.strip(),
                        last_name.strip(),
                        company.strip(),
                        city.strip(),
                        country.strip(),
                        email.strip()
                    )
                    if updated_rows > 0:
                        load_data.clear()
                        st.success(f"CustomerId {selected_id} 고객 정보가 수정되었습니다.")
                        st.rerun()
                    else:
                        st.error("수정된 행이 없습니다. 다시 확인해주세요.")

    with tab3:
        st.subheader("신규 고객 추가")

        with st.form("insert_customer_form"):
            new_first_name = st.text_input("FirstName *")
            new_last_name = st.text_input("LastName *")
            new_company = st.text_input("Company")
            new_city = st.text_input("City")
            new_country = st.text_input("Country")
            new_email = st.text_input("Email *")

            submitted_new = st.form_submit_button("신규 고객 추가")

            if submitted_new:
                if not new_first_name.strip() or not new_last_name.strip() or not new_email.strip():
                    st.warning("FirstName, LastName, Email은 필수 입력값입니다.")
                else:
                    new_id = insert_customer(
                        new_first_name,
                        new_last_name,
                        new_company,
                        new_city,
                        new_country,
                        new_email
                    )
                    load_data.clear()
                    st.success(f"신규 고객이 추가되었습니다. CustomerId = {new_id}")
                    st.rerun()


# ============================================================
# 메인
# ============================================================
def main():
    with st.spinner("데이터를 불러오는 중..."):
        data = load_data()

    if data is None:
        st.error(f"❌ DB 파일을 찾을 수 없거나 필요한 테이블이 없습니다: `{DB_PATH}`")
        st.info("이 app.py와 같은 폴더에 `chinook.db` 파일을 두고 다시 실행해주세요.")
        st.stop()

    df_inv_full = data["invoices"]
    df_items_full = data["items"]

    st.sidebar.title("🎵 Chinook Analytics")
    st.sidebar.caption("음악 스토어 경영분석 대시보드")
    st.sidebar.markdown("---")

    page = st.sidebar.radio(
        "📑 페이지 선택",
        ["📊 매출 Overview", "🌍 고객 & 지역", "🎵 장르 & 상품", "👤 영업사원 성과", "🧾 고객 관리"],
    )

    if page != "🧾 고객 관리":
        st.sidebar.markdown("---")
        st.sidebar.subheader("🔍 공통 필터")

        min_year = int(df_inv_full["Year"].min())
        max_year = int(df_inv_full["Year"].max())
        year_range = st.sidebar.slider(
            "연도 범위",
            min_value=min_year,
            max_value=max_year,
            value=(min_year, max_year),
            step=1,
        )

        all_countries = sorted(df_inv_full["Country"].dropna().unique().tolist())
        countries = st.sidebar.multiselect(
            "국가 선택 (전체 = 비워두기)",
            options=all_countries,
            default=[],
            placeholder="국가를 선택하세요",
        )

        df_inv_filtered = apply_filters(df_inv_full, year_range, countries)
        df_items_filtered = apply_filters(df_items_full, year_range, countries)

        st.sidebar.markdown("---")
        st.sidebar.markdown(
            f"""
            **현재 선택**
            - 기간: {year_range[0]}~{year_range[1]}
            - 국가: {len(countries) if countries else '전체'}
            - 주문: {len(df_inv_filtered):,}건
            - 매출: {format_currency(df_inv_filtered['Total'].sum())}
            """
        )

        if page == "📊 매출 Overview":
            page_overview(df_inv_filtered, df_inv_full)
        elif page == "🌍 고객 & 지역":
            page_customers(df_inv_filtered)
        elif page == "🎵 장르 & 상품":
            page_genres(df_items_filtered)
        elif page == "👤 영업사원 성과":
            page_sales_rep(df_inv_filtered)

    else:
        page_customer_management()

    st.markdown("---")
    st.caption("📚 Chinook Sample Database | Built with Streamlit + Plotly")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"⚠️ 오류가 발생했습니다: {e}")
        st.exception(e)
