import {SearchBox} from "./search/SearchBox";
import Table from "./table/Table";
import React, {Component} from "react";
import Loading from "./Loading";
import ErrorContainer from "./Error";
import Button from "@mui/material/Button";
import ChartWrapper from "./chart/Chart";
import ChartProperties from "./chart/ChartProperties";
import axios from "axios";
import './MainContainer.scss';
import {Paper} from "@material-ui/core";

const VERSION_NUMBER = '2.0.42';
const VOICE_COMMANDS = Object.freeze(
    {
        "TABLE": "table",
        "PROPERTIES": "properties",
        "GO": "go",
        "BAR_CHART": "bar chart",
        "LINE_CHART": "line chart"
    })

export default class MainContainer extends Component {

    constructor(props) {
        super(props);
        this.state = {
            isSubmitting: false,
            error: '',
            tableResponse: {},
            prevQuery: '',
            shouldShowChartProperties: false,
            input: '',
            filters: [],
            chartType: 'LineChart',
            shouldShowChart: true,
        }
    }

    handleInput = (input) => {
        this.setState({input});
    }

    handleError = (error) => {
        this.setState({error});
    }

    handleFilter = () => {
        const {tableResponse, filters} = this.state;
        const rows = tableResponse.data.distinct_values_table.rows
        const selected = filters.map(i => rows[i - 1])
        const input = selected[0].value;
        this.setState({input: selected[0].value});
        this.submit(input);
    }

    handleChartSwitch = () => {
        const {chartType} = this.state;
        if (chartType === 'LineChart')
            this.setState({chartType: 'ColumnChart'});
        else
            this.setState({chartType: 'LineChart'});
    }

    submit = (inputValue) => {
        const {prevQuery} = this.state;
        if (inputValue.toLowerCase() === 'properties') {
            this.setState({shouldShowChartProperties: true});
            return;
        }
        this.setState({isSubmitting: true});
        axios.post('http://localhost:1234/answerz', {
            'text': inputValue.replace('  ', ' '),
            'prev_query': (inputValue.toLowerCase().includes('group') || inputValue.toLowerCase().includes('down by') || inputValue.toLowerCase().includes('out by')) ? prevQuery : ''
        })
            .then(response => {
                this.setState({
                    tableResponse: response,
                    isSubmitting: false,
                    error: ''
                });

                if (!response.data.follow_up) {
                    this.setState({prevQuery: inputValue});

                }
            }).catch((error) => {
            this.setState({isSubmitting: false, error: error});
        });
    }

    submit2 = () => {
        const {input} = this.state;
        if (Object.values(VOICE_COMMANDS).includes(input.toLowerCase())) {
            this.handleVoiceCommands(input);
        } else {
            this.setState({isSubmitting: true});
            axios.post('http://localhost:1234/answerz', {'text': input})
                .then(response => {
                    this.setState({
                        tableResponse: response,
                        isSubmitting: false,
                        error: ''
                    });

                }).catch((error) => {
                this.setState({
                    isSubmitting: false,
                    error: error,
                });
            });
        }

    }

    handleVoiceCommands = (command) => {
        switch (command) {
            case VOICE_COMMANDS.TABLE:
                this.setState({shouldShowChart: false});
                break;
            case VOICE_COMMANDS.PROPERTIES:
                this.setState({shouldShowChartProperties: true});
                break;
            case VOICE_COMMANDS.GO:
                this.handleFilter();
                break;
            case VOICE_COMMANDS.BAR_CHART:
                this.setState({chartType: 'ColumnChart'});
                break;
            case VOICE_COMMANDS.LINE_CHART:
                this.setState({chartType: 'LineChart'});
                break;

        }
    }

    openChartProperties = () => {
        this.setState({shouldShowChartProperties: true});
    }

    closeChartProperties = () => {
        this.setState({shouldShowChartProperties: false});
    }

    handleFilterChanged = (newSelection) => {
        this.setState({filters: newSelection});
    }

    renderDistinctValueTable = () => {
        const {tableResponse} = this.state;
        return (
            <div className="app-container__distinct-values">
                <div className="app-container__distinct-values__label">
                    Choose One or more
                </div>
                <div className="app-container__table">
                    <Table
                        rows={tableResponse.data.distinct_values_table.rows}
                        columns={tableResponse.data.distinct_values_table.cols}
                        pageSize={5}
                        showSelection={true}
                        onFilterChanged={this.handleFilterChanged}
                    />
                </div>
                <div className="app-container__distinct-values__buttons">
                    <Button variant="contained"
                            onClick={this.handleFilter}>Go
                    </Button>
                    <Button Button variant="contained"
                            onClick={this.openChartProperties}>Table Properties
                    </Button>
                </div>
            </div>
        );
    }

    renderTableAndChart = () => {
        const {tableResponse, prevQuery, shouldShowChart, chartType} = this.state;

        return (
            <>
                {
                    tableResponse.data.tables.map((value) =>
                        <div className="app-container__table-and-chart">
                            <div className="app-container__table-and-chart__query">
                                {
                                    tableResponse.data.follow_up ?
                                        <div>
                                            <div>{prevQuery}</div>
                                            <h4>{tableResponse.data.query}</h4>
                                        </div>
                                        :
                                        <div
                                            className="app-container__table-and-chart__query__header">{tableResponse.data.query}</div>
                                }
                                <div className="app-container__table-and-chart__query__total">
                                    {value.total}
                                </div>
                            </div>
                            {value.total > 0 ?
                                <>
                                    {
                                        shouldShowChart && <>
                                            <div className="app-container__table-and-chart__chart">
                                                <ChartWrapper chartType={chartType} data={value.chart_data}/>
                                            </div>
                                            <div className="app-container__table-and-chart__buttons">
                                                <Button
                                                    variant={"contained"}
                                                    onClick={this.openChartProperties}>Chart Properties
                                                </Button>
                                                <Button
                                                    variant={"contained"}
                                                    onClick={this.handleChartSwitch}>Switch chart
                                                </Button>
                                            </div>
                                        </>
                                    }
                                    <div className="app-container__table-and-chart__table">
                                        <Table
                                            rows={value.rows}
                                            columns={value.cols}
                                            pageSize={100}
                                            showSelection={false}
                                            onFilterChanged={this.handleFilterChanged}
                                        />
                                    </div>
                                </>
                                : ''
                            }
                        </div>)
                }
            </>
        );
    }


    render() {

        const {input, tableResponse, isSubmitting, error, shouldShowChartProperties} = this.state;

        return (
            <div className="app-container">
                <div className="app-container__version">
                    {VERSION_NUMBER}
                </div>
                <Paper elevation={1}>
                    <div className="app-container__search">
                        <SearchBox
                            input={input}
                            submit={this.submit}
                            submit2={this.submit2}
                            onInputChanged={this.handleInput}
                            onError={this.handleError}
                        />
                    </div>
                </Paper>
                {
                    isSubmitting && <Loading className="app-container__loading"/>
                }
                {
                    error !== '' && <ErrorContainer/>
                }
                {
                    !isSubmitting &&
                    tableResponse.data &&
                    tableResponse.data.distinct_values
                    &&
                    tableResponse.data.distinct_values_table.rows.length
                    &&
                    this.renderDistinctValueTable()
                }

                {
                    !isSubmitting &&
                    tableResponse.data &&
                    !tableResponse.data.distinct_values_table.rows.length
                    &&
                    this.renderTableAndChart()
                }

                {
                    !isSubmitting &&
                    shouldShowChartProperties &&
                    <ChartProperties
                        shouldOpenChartProperties={shouldShowChartProperties}
                        handleCloseChartProperties={this.closeChartProperties}
                        tables={tableResponse.data.tables}
                        query={tableResponse.data.query}
                        properties={tableResponse.data.chart_properties}
                    />
                }


            </div>
        )
            ;
    }


}