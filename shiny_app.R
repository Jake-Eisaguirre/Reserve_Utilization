library(shiny)
library(DBI)
library(odbc)
library(tidyverse)
library(scales)
library(padr)
library(DT)
library(plotly)
library(ggplot2)
library(lubridate)
library(shinycssloaders)
library(here)
library(dbplyr)
library(shinyWidgets)


# Define UI
ui <- fluidPage(
  tags$head(
    tags$style(HTML("
      body {
        margin: 0;
      }
      
      .title-banner {
        background-color: #413691;
        color: white;
        padding: 15px;
        position: relative;
      }
      
      .title-banner img {
        position: absolute;
        top: 7px;
        right: 60px;
        width: 200px;
        height: 85px; /* Adjust the size of the image */
      }
      
      .main-panel {
        background-color: #f0f0f0; /* Neutral professional grey for the main content area */
        padding: 15px; /* Padding for spacing inside the main panel */
      }
      
      .sidebar-panel {
        background-color: #d0d0d0; /* Darker grey for sidebar panel */
        padding: 15px; /* Optional padding */
      }
      
      .solid-line {
        border-top: 2px solid #cccccc; /* Solid line separator */
        margin: 20px 0; /* Space above and below the line */
      }
      
            /* Custom styles for spinners */
      .shiny-spinner {
        border: 4px solid #f0f0f0; /* Light grey border for spinner */
        border-top-color: #ff66b2; /* Pink color for spinner top */
      }

      .spinner-border-sm {
        border: 2px solid #f0f0f0; /* Light grey border for small spinner */
        border-top-color: #ff66b2; /* Pink color for spinner top */
      }
      
      .spinner-border-xl {
        border: 6px solid #f0f0f0; /* Light grey border for extra-large spinner */
        border-top-color: #ff66b2; /* Pink color for spinner top */
      }
      
      .selectize-dropdown-content .active {
        background-color: #D2058A;
      
      }
      
      .shiny-input-select {
      
       color: #D2058A;
      
      }
      
      .nav-tabs > li > a {
        color: #D2058A;
      }
      

    "))
  ),
  
  # Title banner with image
  div(class = "title-banner",
      tags$h1("Flight Attendant Reserve Utilization"),
      img(src = "OIP.png", alt = "Image", class = "banner-image")
  ),
  
  div(
    h3("Data Background"),
    HTML("
    Data presented in this report is aggregated across the <strong>CT_MASTER_HISTORY</strong> table from the Snowflake Database within the <strong>CREW ANALYTICS</strong> Schema.
    For the purpose of this report, reserve utilization is defined as an employee having an <strong>RLV</strong> code and receiving an <strong>ASN</strong> code for a given day.
    The <strong>RLV</strong> head count per day is determined by the count of distinct employees per day on the 25th of the preceding bid period; This is prior to when FAs can trade RLV days.
    The black dashed line presented on the first figure indicates the average utilization for the given bid period.
    For the sick code figure and data; sick codes are defined as <strong>SOP</strong>, <strong>2SK</strong>, <strong>FLV</strong>, <strong>FLP</strong>, <strong>UNA</strong>, <strong>FLS</strong>, <strong>FLU</strong>, <strong>N/S</strong>, <strong>PER</strong>, <strong>MGR</strong>.
    The sick code figure visualizes the final sick code associated with the employee following an assignment determined by the greatest value associated for Update Date and Time column per day per employee.
    The sick code table provides all the sick codes within the transaction following the assignment.
  "),
    tags$hr(class = "solid-line")),
  
  div(h3("Daily Reserve Utilization and Sick Codes")),
  
  sidebarLayout(
    sidebarPanel(width=2,
      class = "sidebar-panel",
      selectInput("bid_periods_input", "Bid Period", choices = NULL),
      selectInput("bases", "Base", choices = c("HNL", "LAX")),
      selectInput("sick_input", "Sick Code", choices = c("SOP", "2SK", "FLV", "FLP", 
                                                         "UNA", "FLS", "FLU", "N/S", 
                                                         "PER", "MGR"), 
                  multiple = T,
                  selected = c("2SK"))
    ),
    
    mainPanel(width = 9,
      class = "main-panel",
      tabsetPanel(
        tabPanel("Reserve Utilization", 
                 withSpinner(plotlyOutput('plot_utl', height = "600px"), color = getOption("spinner.color", default = "#D2058A")),
                 tags$hr(class = "solid-line")
        ),
        tabPanel("Sick After ASN", 
                 withSpinner(plotlyOutput('plotly_sic', height = "400px"), color = getOption("spinner.color", default = "#D2058A")),
                 tags$hr(class = "solid-line"),
                 withSpinner(dataTableOutput('asn_table'), color = getOption("spinner.color", default = "#D2058A")),
                 tags$br()
        )
      )
    )
  )
)


# Define server logic
server <- function(input, output, session) {
  
  
  raw_date <- Sys.Date()
  
  fut_bid_period <- substr(as.character((raw_date +30)), 1, 7)
  
  # Database connection
  db_connection <- dbConnect(odbc::odbc(),
                             Driver = "SnowflakeDSIIDriver",
                             Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",
                             WAREHOUSE = "DATA_LAKE_READER",
                             Database = "ENTERPRISE",
                             UID = "jacob.eisaguirre@hawaiianair.com", 
                             authenticator = "externalbrowser")
  
  dbExecute(db_connection, "USE SCHEMA CREW_ANALYTICS")
  
  
  # Fetch bid periods for selectInput
  q_bid_periods <- "SELECT DISTINCT BID_PERIOD FROM CT_MASTER_HISTORY ORDER BY BID_PERIOD DESC;"
  bid_periods <- dbGetQuery(db_connection, q_bid_periods)%>% 
    filter(!BID_PERIOD == fut_bid_period)
  
  
  updateSelectInput(session, "bid_periods_input", choices = bid_periods$BID_PERIOD)
  

  
  bid_period_rec <- reactive({
    
    # Fetch master history data
    q_master_history <- paste0("SELECT * FROM CT_MASTER_HISTORY WHERE BID_PERIOD='", input$bid_periods_input,"';")
    
    master_history_raw <- dbGetQuery(db_connection, q_master_history) %>%
      mutate(UPDATE_TIME = as.character(UPDATE_TIME),
             UPDATE_DATE = as.character(UPDATE_DATE)) 
  })

  
  

  reserve_utl <- reactive({
    
    update_dt_rlv <- paste0((as_datetime(paste0(input$bid_periods_input, "-25 00:00:00"))-2592000),  "-25 00:00:00") 

    
    fa_ut_rlv <- bid_period_rec() %>% 
      ungroup() %>% 
      filter(CREW_INDICATOR == "FA",
             #BID_PERIOD == input$bid_periods_input
      ) %>% 
      filter(TRANSACTION_CODE %in% c("ACR", "RSV", "RLV", "ASN", "BSN", "BRD")) %>%
      mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%
      filter(update_dt < update_dt_rlv) %>%
      group_by(CREW_ID, PAIRING_DATE, TRANSACTION_CODE) %>%
      mutate(temp_id = cur_group_id()) %>%
      filter(!duplicated(temp_id)) %>%
      ungroup() %>% 
      select(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_NO,
             PAIRING_DATE, TO_DATE, PAIRING_POSITION, BID_PERIOD, BASE, update_dt)
  
  
    
    
    fa_ut_asn <- bid_period_rec() %>% 
      ungroup() %>% 
      filter(CREW_INDICATOR == "FA",
             BID_PERIOD == input$bid_periods_input) %>% 
      filter(TRANSACTION_CODE %in% c("ACR", "RSV", "RLV", "ASN", "BSN", "BRD")) %>% 
      mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%
      group_by(CREW_ID, PAIRING_DATE, TRANSACTION_CODE) %>%
      mutate(temp_id = cur_group_id()) %>%
      filter(!duplicated(temp_id)) %>%
      ungroup() %>% 
      select(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_NO,
             PAIRING_DATE, TO_DATE, PAIRING_POSITION, BID_PERIOD, BASE, update_dt)
    
    fa_ut_single <- fa_ut_asn %>% 
      group_by(CREW_ID, PAIRING_NO) %>% 
      mutate(single = if_else(PAIRING_DATE == TO_DATE, 1, 0)) %>% 
      filter(single == 1) %>% 
      filter(TRANSACTION_CODE %in% c("ASN", "BSN", "BRD")) %>% 
      pivot_longer(cols = c("PAIRING_DATE", "TO_DATE"),
                   values_to = "DATE") %>% 
      group_by(CREW_ID, TRANSACTION_CODE, DATE, PAIRING_NO) %>% 
      mutate(temp_id = cur_group_id()) %>% 
      filter(!duplicated(temp_id)) %>%
      ungroup()
    
    fa_ut_double <- fa_ut_asn %>% 
      group_by(CREW_ID, PAIRING_NO) %>% 
      mutate(single = if_else(PAIRING_DATE == TO_DATE, 1, 0)) %>% 
      filter(single == 0) %>% 
      filter(TRANSACTION_CODE %in% c("ASN", "BSN", "BRD")) %>% 
      pivot_longer(cols = c("PAIRING_DATE", "TO_DATE"),
                   values_to = "DATE") %>% 
      group_by(CREW_ID, TRANSACTION_CODE, DATE, PAIRING_NO) %>% 
      mutate(temp_id = cur_group_id()) %>% 
      filter(!duplicated(temp_id)) %>%
      ungroup() %>% 
      group_by(CREW_ID, BASE, PAIRING_NO) %>% 
      pad() %>% 
      ungroup()
    
    piv_emp_hist_fa_asn <- rbind(fa_ut_double, fa_ut_single) %>% 
      group_by(DATE, BASE) %>% 
      mutate(rlv_used = n()) %>% 
      ungroup() %>% 
      select(DATE, BASE, rlv_used)
    
    
    piv_emp_hist_fa_rlv <- fa_ut_rlv %>% 
      mutate(update_dt = as_datetime(update_dt)) %>% 
      filter(TRANSACTION_CODE %in% c("RLV")) %>%
      rename(DATE = PAIRING_DATE) %>% 
      group_by(DATE, BASE) %>% 
      mutate(net_rlv_available = length(unique(CREW_ID))) %>% 
      ungroup() %>% 
      select(DATE, BASE, net_rlv_available)
    
    
    comb_fa <- left_join(piv_emp_hist_fa_rlv, piv_emp_hist_fa_asn, relationship = "many-to-many",
                         by = join_by(DATE, BASE)) %>% 
      group_by(DATE, BASE) %>% 
      mutate(temp_id = cur_group_id()) %>% 
      filter(!duplicated(temp_id)) %>% 
      select(!temp_id) %>%
      ungroup() %>% 
      mutate(rlv_used = if_else(is.na(rlv_used), 0, rlv_used)) %>% 
      mutate(rlv_remaining = net_rlv_available - rlv_used)%>% 
      #mutate(rlv_remaining = if_else(rlv_remaining < 0,0, rlv_remaining)) %>% 
      mutate(perc_ut = round((rlv_used/net_rlv_available)*100, 0)) %>% 
      group_by(BASE) %>% 
      mutate(m_avg_utl = round(mean(rlv_used), 0),
             m_perc_utl = paste0(round(mean(perc_ut), 0), "%"),
             perc_ut = paste0(perc_ut, "%"))
    
    comb_fa_hnl <- comb_fa %>%
      filter(BASE == input$bases)
    
  })
  
  
  long_data_fa_hnl_rec <- reactive({
    
    reserve_utl() %>%
      select(DATE, BASE, rlv_used, rlv_remaining) %>%
      pivot_longer(cols = c(rlv_used, rlv_remaining), 
                   names_to = "rlv_type", 
                   values_to = "Head_Count") %>% 
      filter(BASE == input$bases) %>% 
      mutate(rlv_type=if_else(rlv_type == "rlv_remaining", "RLV Available", "RLV Utilized"))
    
    
  })
  
  
  
  label_data_hnl_rec <- reactive({
    
    reserve_utl() %>%
      select(DATE, BASE, net_rlv_available)%>% 
      filter(BASE == input$bases)
    
    
  })
  
  
  
  label_data_hnl_used_rec <- reactive({
    
    reserve_utl() %>%
      select(DATE, BASE, rlv_used, perc_ut)%>% 
      filter(BASE == input$bases)
    
  })
  
  
  hline_reac <- reactive({
    
    data.frame(
      yintercept = reserve_utl()$m_avg_utl)
    
  })
  
  tot_rlv_label_rec <- reactive({
    
    fa_ut_rlv <- bid_period_rec() %>% 
      ungroup() %>% 
      filter(CREW_INDICATOR == "FA",
             #BID_PERIOD == input$bid_periods_input
      ) %>% 
      filter(TRANSACTION_CODE %in% c("RLV")) %>%
      mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%
      filter(update_dt < update_dt_rlv) %>%
      group_by(CREW_ID, PAIRING_DATE, TRANSACTION_CODE) %>%
      mutate(temp_id = cur_group_id()) %>%
      filter(!duplicated(temp_id)) %>%
      ungroup() %>% 
      select(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_NO,
             PAIRING_DATE, TO_DATE, PAIRING_POSITION, BID_PERIOD, BASE, update_dt) %>% 
      reframe(tot = paste(length(unique(CREW_ID)), "Crew"))
    
    
  }) 
  
  
  output$plot_utl <- renderPlotly({
    
    # Get the data first from the reactive expression
    reserve_data <- reserve_utl()
    
    # Create the ggplot object
    utl_p <- ggplot(reserve_utl()) +
      geom_bar(aes(x = DATE, y = net_rlv_available,
                   text = paste("Date:", DATE, "<br>RLV Available:", net_rlv_available)), 
               stat = "identity", alpha = 0.3) +
      geom_bar(data = long_data_fa_hnl_rec(), 
               aes(x = DATE, y = Head_Count, fill = rlv_type, 
                   text = paste0("Date:", DATE, "<br>", rlv_type, ": ", Head_Count)), 
               stat = "identity", position = "stack") +
      scale_x_date(date_labels = "%Y-%m-%d", date_breaks = "3 days") +
      scale_fill_manual(values = c("RLV Available" = "#413691", 
                                   "RLV Utilized" = "#D2058A")) +
      labs(x = "Date", y = "Primary RLV Head Count", fill = "RLV Type") +
      theme_minimal() +
      theme(axis.text.x = element_text(angle = 45, hjust = 1),
            legend.text = element_text(size = 9),
            legend.key.size = unit(0.35, "cm"),
            legend.title = element_text(size = 5),
            legend.box.spacing = unit(0.35, "cm"),
            plot.title = element_text(hjust = 1.5)) +
      ggtitle(paste(input$bases, "-", tot_rlv_label_rec()$tot)) +
      geom_text(data = label_data_hnl_rec(),
                aes(x = DATE, y = net_rlv_available  + (0.025 * max(net_rlv_available)), label = net_rlv_available),
                size = 2.25, vjust = -0.75, hjust = 0.5) +
      geom_text(data = label_data_hnl_used_rec(),
                aes(x = DATE, y = (rlv_used + (0.025 * max(rlv_used))), label = perc_ut),
                size = 2.25, vjust = -0.75, hjust = 0.5, color = "white") +
      geom_hline(aes(yintercept = mean(reserve_data$m_avg_utl)), linetype = "dashed", color = "black") + # Fix here
      geom_text(aes(x = max(reserve_data$DATE) + 1.25, y = max(reserve_data$m_avg_utl)*1.1, 
                    label = m_perc_utl), vjust = -1, hjust = 0, color = "black") + 
      coord_cartesian(ylim = c(0, NA)) 
    
    ggplotly(utl_p, tooltip = "text")
  })
  
  
  ####################### Sick After ASN ######################################
  fa_trx_hist_rec <- reactive({
    
    # # Fetch master history data
    # q_master_history <- paste0("SELECT * FROM CT_MASTER_HISTORY WHERE BID_PERIOD='", input$bid_periods_input,"';")
    # bid_period_rec() <- dbGetQuery(db_connection, q_master_history) %>%
    #   mutate(UPDATE_TIME = as.character(UPDATE_TIME),
    #          UPDATE_DATE = as.character(UPDATE_DATE))
    
    
    bid_period_rec() %>% 
      filter(CREW_INDICATOR == "FA",
             #BID_PERIOD == input$bid_periods_input,
             BASE == input$bases) %>% 
      mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>% 
      group_by(CREW_ID, PAIRING_DATE, TRANSACTION_CODE) %>% 
      mutate(temp_id = cur_group_id()) %>% 
      filter(!duplicated(temp_id)) %>% 
      select(!temp_id) %>% 
      ungroup() %>% 
      group_by(CREW_ID, PAIRING_DATE) %>% 
      mutate(keep = ifelse(any(TRANSACTION_CODE %in% c("SCR", "ASN", "RSV", "ARC", "RLV")), 1, 0)) %>% 
      filter(keep == 1)
    
    
  })
  
  sick_follow_asn_rec <- reactive({
    
    fa_trx_hist_rec() %>% 
      group_by(CREW_ID, PAIRING_DATE) %>% 
      mutate(keep = if_else(any(TRANSACTION_CODE %in% c("SOP", "2SK", "FLV", "FLP", "UNA", "FLS", "FLU",
                                                        "N/S", "PER", "MGR")), 1, 0)) %>% 
      filter(keep == 1)
    
    
  }) 
  
  
  sum_fa_rsk_sop_rec <- reactive({
    
    sick_follow_asn_rec() %>% 
      group_by(CREW_ID, PAIRING_DATE) %>% 
      mutate(keep = if_else(any(TRANSACTION_CODE %in% c("ASN")), 1, 0)) %>%
      filter(keep == 1,
             TRANSACTION_CODE %in% c("SOP", "2SK", "FLV", "FLP", "UNA", "FLS", "FLU",
                                     "N/S", "PER", "MGR")) %>% 
      dplyr::arrange(CREW_ID, PAIRING_DATE, update_dt) %>% 
      filter(update_dt == max(update_dt)) %>% 
      group_by(PAIRING_POSITION, BASE, PAIRING_DATE, TRANSACTION_CODE) %>% 
      summarise(rlv_asn_2sk = if_else(TRANSACTION_CODE == "2SK", length(unique(CREW_ID)), NA),
                rlv_asn_sop = if_else(TRANSACTION_CODE == "SOP", length(unique(CREW_ID)), NA),
                rlv_asn_flv = if_else(TRANSACTION_CODE == "FLV", length(unique(CREW_ID)), NA),
                rlv_asn_flp = if_else(TRANSACTION_CODE == "FLP", length(unique(CREW_ID)), NA),
                rlv_asn_una = if_else(TRANSACTION_CODE == "UNA", length(unique(CREW_ID)), NA),
                rlv_asn_fls = if_else(TRANSACTION_CODE == "FLS", length(unique(CREW_ID)), NA),
                rlv_asn_flu = if_else(TRANSACTION_CODE == "FLU", length(unique(CREW_ID)), NA),
                rlv_asn_ns = if_else(TRANSACTION_CODE == "N/S", length(unique(CREW_ID)), NA),
                rlv_asn_per = if_else(TRANSACTION_CODE == "PER", length(unique(CREW_ID)), NA),
                rlv_asn_mgr = if_else(TRANSACTION_CODE == "MGR", length(unique(CREW_ID)), NA)) %>% 
      ungroup() %>% 
      select(PAIRING_POSITION, BASE, rlv_asn_2sk, rlv_asn_sop, PAIRING_DATE, rlv_asn_flv, rlv_asn_flp,
             rlv_asn_una, rlv_asn_fls, rlv_asn_flu, rlv_asn_ns, rlv_asn_per, rlv_asn_mgr )  %>% 
      group_by(PAIRING_POSITION, BASE, rlv_asn_2sk, rlv_asn_sop, PAIRING_DATE) %>% 
      # mutate(temp_id = cur_group_id()) %>% 
      # filter(!duplicated(temp_id)) %>% 
      # select(!temp_id) %>% 
      ungroup()
    
    
  }) 
  
  
  # Reshape data to long format
  long_data_deg_fa_hnl_rec <- reactive({
    
    sum_fa_rsk_sop_rec() %>%
      select(PAIRING_DATE, BASE, rlv_asn_2sk, rlv_asn_sop, rlv_asn_flv, rlv_asn_flp,
             rlv_asn_una, rlv_asn_fls, rlv_asn_flu, rlv_asn_ns, rlv_asn_per, rlv_asn_mgr) %>%
      pivot_longer(cols = c(rlv_asn_2sk, rlv_asn_sop, rlv_asn_flv, rlv_asn_flp,
                            rlv_asn_una, rlv_asn_fls, rlv_asn_flu, rlv_asn_ns, rlv_asn_per, rlv_asn_mgr), 
                   names_to = "sick_type", 
                   values_to = "Head_Count") %>% 
      drop_na(Head_Count) %>% 
      mutate(sick_type=case_when(sick_type == "rlv_asn_2sk" ~ "2SK",
                                 sick_type == "rlv_asn_sop" ~ "SOP",
                                 sick_type == "rlv_asn_flv" ~ "FLV",
                                 sick_type == "rlv_asn_flp" ~ "FLP",
                                 sick_type == "rlv_asn_una" ~ "UNA",
                                 sick_type == "rlv_asn_fls" ~ "FLS",
                                 sick_type == "rlv_asn_flu" ~ "FLU",
                                 sick_type == "rlv_asn_ns" ~ "N/S",
                                 sick_type == "rlv_asn_per" ~ "PER",
                                 sick_type == "rlv_asn_mgr" ~ "MGR")) %>% 
      group_by(PAIRING_DATE, BASE, sick_type) %>% 
      mutate(temp_id = cur_group_id()) %>% 
      filter(!duplicated(temp_id),
             sick_type %in% c(input$sick_input))
    
    
  }) 
  
  
  
  label_data_fa_rsk_hnl_rec <- reactive({
    
    sum_fa_rsk_sop_rec() %>%
      ungroup() %>% 
      select(PAIRING_DATE, BASE, rlv_asn_2sk, rlv_asn_sop, rlv_asn_flv, rlv_asn_flp,
             rlv_asn_una, rlv_asn_fls, rlv_asn_flu, rlv_asn_ns, rlv_asn_per, rlv_asn_mgr) %>% 
      group_by(PAIRING_DATE, BASE) %>% 
      mutate(sum_codes = sum(rlv_asn_2sk, rlv_asn_sop, rlv_asn_flv, rlv_asn_flp,
                             rlv_asn_una, rlv_asn_fls, rlv_asn_flu, rlv_asn_ns, rlv_asn_per, rlv_asn_mgr, na.rm = T))
    
    
  }) 
  
  label_data_fa_sop_hnl_rec <- reactive({
    
    sum_fa_rsk_sop_rec() %>%
      ungroup() %>% 
      select(PAIRING_DATE, BASE, rlv_asn_sop) %>% 
      drop_na(rlv_asn_sop) 
    
    
  }) 
  
  custom_colors_rec  <- reactive({
    
    c(colorRampPalette(c("#413691", "#D2058A"))(length(unique(long_data_deg_fa_hnl_rec()$sick_type))))
    
  }) 
  
  
  output$plotly_sic <- renderPlotly({
    
    code_p <- ggplot(sum_fa_rsk_sop_rec()) +
      geom_bar(data = long_data_deg_fa_hnl_rec(), 
               aes(x = PAIRING_DATE, y = Head_Count, fill = sick_type), 
               stat = "identity", position = "stack") +
      scale_x_date(date_labels = "%Y-%m-%d", date_breaks = "3 days") +
      scale_fill_manual(values = custom_colors_rec(), name = "RLV Sick Type") +
      labs(x = "Date", y = "Primary Head Count", fill = "RLV Sick Type") +
      theme_minimal() +
      theme(axis.text.x = element_text(angle = 45, hjust = 1),
            legend.text = element_text(size = 6),  # Adjust legend text size
            legend.key.size = unit(0.25, "cm"),  # Adjust size of legend keys
            legend.title = element_text(size = 5),  # Adjust legend title size
            legend.box.spacing = unit(0.25, "cm"),
            plot.title = element_text(hjust = 0.5)# Adjust spacing around the legend box
      ) +
      ggtitle(paste(input$bases))
    
    ggplotly(code_p)
  })
  
  
  
  sum_fa_rsk_sop_t_hnl_rec <- reactive({
    
    sick_follow_asn %>% 
      filter(BASE == input$bases) %>% 
      group_by(CREW_ID, PAIRING_DATE) %>% 
      mutate(keep = if_else(any(TRANSACTION_CODE %in% c("ASN")), 1, 0)) %>%
      filter(keep == 1) %>% 
      filter(TRANSACTION_CODE %in% c("ASN", "2SK", "SOP", "FLV", "FLP", "UNA", "FLS", "FLU",
                                     "N/S", "PER", "MGR")) %>% 
      select(CREW_ID, TRANSACTION_CODE, PAIRING_DATE, update_dt) %>% 
      arrange(PAIRING_DATE, CREW_ID, update_dt) %>% 
      rename(UPDATE_DATE_TIME = update_dt)
    
    
  }) 
  
  output$asn_table <- renderDataTable({
    
    sum_fa_rsk_sop_t_hnl_rec() %>% 
      datatable()
    
    
  })
  
}

# Run the application 
shinyApp(ui = ui, server = server)
