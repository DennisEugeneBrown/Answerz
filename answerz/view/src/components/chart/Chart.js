import React from "react";
import {Chart} from "react-google-charts";
import Loading from "../Loading";
import {Paper} from "@material-ui/core";

export default function ChartWrapper({data, chartType}) {

    return (
        <Paper sx={{width: '100%'}}>
            <Chart
                width={'100%'}
                height={'600px'}
                chartType={chartType}
                loader={<Loading/>}
                data={data}
                options={{
                    titleTextStyle: {
                        fontSize: 16,
                    },
                    annotations: {
                        textStyle: {
                            fontSize: 16,
                        }
                    },
                    legend: {
                        position: "bottom",
                        alignment: "start",
                        maxLines: 2,
                        textStyle: {fontSize: 16}
                    },
                    hAxis: {
                        title: '',
                        titleTextStyle: {
                            color: "#000",
                            fontName: "sans-serif",
                            fontSize: 16,
                            bold: true,
                            italic: false
                        }
                    },
                    vAxis: {
                        title: '',
                        titleTextStyle: {
                            color: "#000",
                            fontName: "sans-serif",
                            fontSize: 16,
                            bold: true,
                            italic: false
                        }
                    },
                }}
            />
        </Paper>
    )
}