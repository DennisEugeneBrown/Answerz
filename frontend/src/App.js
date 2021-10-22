import logo from './logo.svg';
import spinner from './spinner.svg';
import mic from './mic.svg'
import voice from './voice.png';
import './App.css';
import React, {useEffect, useState, useReducer} from 'react';
import 'react-minimal-side-navigation/lib/ReactMinimalSideNavigation.css';
import axios from 'axios'
import {JsonToTable} from "react-json-to-table";
import {Container, Row, Col} from 'react-grid-system';
import LoadingIcons from 'react-loading-icons'

const formReducer = (state, event) => {
    return {
        ...state,
        [event.name]: event.value
    }
}

function App() {
    const [getMessage, setGetMessage] = useState({})
    const [getTable, setGetTable] = useState({})
    const [formData, setFormData] = useReducer(formReducer, {});
    const [submitting, setSubmitting] = useState(false);
    const [getError, setError] = useState(false);
    const [getColumns, setColumns] = useState([]);
    const [getDataTables, setDataTables] = useState([]);
    const [getState, setState] = useState({
        leftOpen: true,
        rightOpen: true,
    });

    useEffect(() => {
        axios.get('http://localhost:5000/columns').then(response => {
            setColumns(response)
        }).catch(error => {
            console.log(error)
        })
    }, [])


    useEffect(() => {
        axios.get('http://localhost:5000/tables').then(response => {
            setDataTables(response)
        }).catch(error => {
            console.log(error)
        })


    }, [])

    const handleSubmit = event => {
        event.preventDefault();
        setSubmitting(true);
        axios.post('http://localhost:5000/answerz', {'text': formData.name})
            .then(response => {
                console.log("Responded", response)
                setGetTable(response)
                setSubmitting(false);
                setError(false);
            }).catch((error) => {
            setSubmitting(false);
            setError(true);
        });
    }

    const handleChange = event => {
        setFormData({
            name: event.target.name,
            value: event.target.value,
        });
    }

    const toggleSidebar = (event) => {
        let key = `${event.currentTarget.parentNode.id}Open`;
        let otherKey = (key === 'leftOpen') ? 'rightOpen' : 'leftOpen';
        setState({[key]: !getState[key], [otherKey]: getState[otherKey]});
        console.log(getState);
    }

    let leftOpen = getState.leftOpen ? 'open' : 'closed';
    let rightOpen = getState.rightOpen ? 'open' : 'closed';

    return (
        <div className="App">
            <div id='layout'>
                <div id='left' className={leftOpen}>
                    <div className='icon'
                         onClick={toggleSidebar}>
                        &equiv;
                    </div>
                    <div className='bordered'>
                        <div className={`sidebar ${leftOpen}`}>
                            <div className='header'>
                                <h2 className='titleLeft'>
                                    Data Panel
                                </h2>
                            </div>
                            <div className='leftContent'>
                                <div className='header'>
                                    <div className='item'>
                                        <h3>Source Table</h3>
                                    </div>
                                </div>
                                <div className='header'>
                                    <div className='item'>
                                        <h3>Data Tables</h3>
                                    </div>
                                </div>
                                {
                                    getDataTables.status === 200 ?
                                        getDataTables.data.map((item, key) => {
                                            return <div className='header'>
                                                <p className='listItem'>{item}</p>
                                            </div>
                                        }) : ''
                                }
                                <div className='header'>
                                    <div className='item'>
                                        <h3>Columns</h3>
                                    </div>
                                </div>
                                {
                                    getColumns.status === 200 ?
                                        getColumns.data.map((item, key) => {
                                            return <div className='header'>
                                                <p className='listItem'>{item}</p>
                                            </div>
                                        }) : ''
                                }
                            </div>
                        </div>
                    </div>
                </div>

                <div id='main'>
                    <div className='header'>
                        <div className={`
                      title
                      ${'left-' + leftOpen}
                      ${'right-' + rightOpen}
                  `}>
                            <Container>
                                <Row>
                                    <Col sm={6}>
                                        <div className='logo'>
                                            <svg width="339" height="70" viewBox="0 0 339 70" fill="none"
                                                 xmlns="http://www.w3.org/2000/svg">
                                                <path fill-rule="evenodd" clip-rule="evenodd"
                                                      d="M35.0002 68.8333C16.3146 68.8333 1.16691 53.6856 1.16691 34.9999C1.16691 16.3143 16.3146 1.1666 35.0002 1.16661C53.6859 1.16661 68.8336 16.3143 68.8336 34.9999C68.8336 53.6856 53.6859 68.8333 35.0002 68.8333ZM35.0002 69.9999C15.6703 69.9999 0.000242451 54.3299 0.000244141 34.9999C0.000245831 15.67 15.6703 -6.2725e-05 35.0002 -6.10352e-05C54.3302 -5.93453e-05 70.0002 15.67 70.0002 34.9999C70.0002 54.3299 54.3302 69.9999 35.0002 69.9999Z"
                                                      fill="url(#paint0_linear_638:280)" fill-opacity="0.18"/>
                                                <path fill-rule="evenodd" clip-rule="evenodd"
                                                      d="M35.0002 61.4457C20.3946 61.4457 8.55444 49.6055 8.55445 34.9999C8.55445 20.3943 20.3946 8.5541 35.0002 8.5541C49.6058 8.5541 61.446 20.3943 61.446 34.9999C61.446 49.6055 49.6058 61.4457 35.0002 61.4457ZM35.0002 62.6123C19.7503 62.6123 7.38778 50.2498 7.38778 34.9999C7.38778 19.7499 19.7503 7.38743 35.0002 7.38744C50.2502 7.38744 62.6127 19.75 62.6127 34.9999C62.6127 50.2498 50.2502 62.6123 35.0002 62.6123Z"
                                                      fill="url(#paint1_linear_638:280)" fill-opacity="0.18"/>
                                                <path fill-rule="evenodd" clip-rule="evenodd"
                                                      d="M35.0003 54.3004C24.3409 54.3004 15.6998 45.6593 15.6998 34.9999C15.6998 24.3406 24.3409 15.6995 35.0003 15.6995C45.6596 15.6995 54.3007 24.3406 54.3007 34.9999C54.3007 45.6593 45.6596 54.3004 35.0003 54.3004ZM35.0003 55.467C23.6966 55.467 14.5331 46.3036 14.5331 34.9999C14.5331 23.6962 23.6966 14.5328 35.0003 14.5328C46.3039 14.5328 55.4674 23.6962 55.4674 34.9999C55.4674 46.3036 46.3039 55.467 35.0003 55.467Z"
                                                      fill="url(#paint2_linear_638:280)" fill-opacity="0.18"/>
                                                <path fill-rule="evenodd" clip-rule="evenodd"
                                                      d="M35.095 47.3381C28.3332 47.3381 22.8517 41.8565 22.8517 35.0947C22.8517 28.3329 28.3332 22.8514 35.095 22.8514C41.8568 22.8514 47.3383 28.3329 47.3383 35.0947C47.3383 41.8565 41.8568 47.3381 35.095 47.3381ZM35.095 48.3739C27.7611 48.3739 21.8159 42.4286 21.8159 35.0947C21.8159 27.7609 27.7612 21.8156 35.095 21.8156C42.4289 21.8156 48.3741 27.7609 48.3741 35.0947C48.3741 42.4286 42.4289 48.3739 35.095 48.3739Z"
                                                      fill="url(#paint3_linear_638:280)" fill-opacity="0.18"/>
                                                <path
                                                    d="M34.9807 37.0706C33.4755 37.0706 32.2506 35.7828 32.2506 34.2004L32.2506 31.3303C32.2506 29.7479 33.4755 28.4601 34.9807 28.4601C36.4859 28.4601 37.7109 29.7479 37.7109 31.3303V34.2004C37.7109 35.7828 36.4859 37.0706 34.9807 37.0706Z"
                                                    fill="url(#paint4_linear_638:280)"/>
                                                <path
                                                    d="M30.9307 32.4804C31.1791 32.4804 31.3808 32.6811 31.3808 32.9304L31.3808 34.2804C31.3808 36.2659 32.9954 37.8806 34.9809 37.8806C36.9664 37.8806 38.5811 36.2659 38.5811 34.2804V32.9304C38.5811 32.6811 38.7827 32.4804 39.0311 32.4804C39.2796 32.4804 39.4812 32.6811 39.4812 32.9304V34.2804C39.4812 36.6071 37.6991 38.5053 35.431 38.7348L35.431 41.0308C35.431 41.2792 35.2294 41.4808 34.9809 41.4808C34.7325 41.4808 34.5309 41.2792 34.5309 41.0308V38.7348C32.2628 38.5053 30.4807 36.6071 30.4807 34.2804L30.4807 32.9304C30.4807 32.6811 30.6823 32.4804 30.9307 32.4804Z"
                                                    fill="url(#paint5_linear_638:280)"/>
                                                <path
                                                    d="M103.291 26.5107L94.9514 45.8011H90.5475L82.2351 26.5107H87.0519L92.9146 40.2896L98.8599 26.5107H103.291Z"
                                                    fill="url(#paint6_linear_638:280)"/>
                                                <path
                                                    d="M113.989 46.1318C111.989 46.1318 110.181 45.7 108.567 44.8366C106.97 43.9731 105.713 42.7881 104.796 41.2816C103.897 39.7568 103.447 38.0482 103.447 36.1559C103.447 34.2636 103.897 32.5642 104.796 31.0578C105.713 29.5329 106.97 28.3387 108.567 27.4753C110.181 26.6118 111.989 26.1801 113.989 26.1801C115.989 26.1801 117.787 26.6118 119.384 27.4753C120.98 28.3387 122.237 29.5329 123.154 31.0578C124.072 32.5642 124.531 34.2636 124.531 36.1559C124.531 38.0482 124.072 39.7568 123.154 41.2816C122.237 42.7881 120.98 43.9731 119.384 44.8366C117.787 45.7 115.989 46.1318 113.989 46.1318ZM113.989 42.3288C115.127 42.3288 116.154 42.0716 117.072 41.5572C117.989 41.0244 118.705 40.2896 119.218 39.3526C119.751 38.4156 120.017 37.3501 120.017 36.1559C120.017 34.9618 119.751 33.8962 119.218 32.9592C118.705 32.0223 117.989 31.2966 117.072 30.7822C116.154 30.2494 115.127 29.983 113.989 29.983C112.851 29.983 111.824 30.2494 110.906 30.7822C109.989 31.2966 109.264 32.0223 108.732 32.9592C108.218 33.8962 107.961 34.9618 107.961 36.1559C107.961 37.3501 108.218 38.4156 108.732 39.3526C109.264 40.2896 109.989 41.0244 110.906 41.5572C111.824 42.0716 112.851 42.3288 113.989 42.3288Z"
                                                    fill="url(#paint7_linear_638:280)"/>
                                                <path d="M127.882 26.5107H132.341V45.8011H127.882V26.5107Z"
                                                      fill="url(#paint8_linear_638:280)"/>
                                                <path
                                                    d="M146.134 46.1318C144.171 46.1318 142.391 45.7092 140.794 44.8641C139.216 44.0007 137.968 42.8157 137.051 41.3092C136.152 39.7843 135.702 38.0666 135.702 36.1559C135.702 34.2453 136.152 32.5367 137.051 31.0302C137.968 29.5053 139.216 28.3204 140.794 27.4753C142.391 26.6118 144.18 26.1801 146.161 26.1801C147.831 26.1801 149.336 26.474 150.675 27.0619C152.033 27.6498 153.171 28.4949 154.089 29.5972L151.226 32.2427C149.923 30.7363 148.308 29.983 146.382 29.983C145.189 29.983 144.125 30.2494 143.189 30.7822C142.253 31.2966 141.519 32.0223 140.987 32.9592C140.473 33.8962 140.216 34.9618 140.216 36.1559C140.216 37.3501 140.473 38.4156 140.987 39.3526C141.519 40.2896 142.253 41.0244 143.189 41.5572C144.125 42.0716 145.189 42.3288 146.382 42.3288C148.308 42.3288 149.923 41.5664 151.226 40.0415L154.089 42.6871C153.171 43.8078 152.033 44.662 150.675 45.2499C149.318 45.8378 147.804 46.1318 146.134 46.1318Z"
                                                    fill="url(#paint9_linear_638:280)"/>
                                                <path
                                                    d="M172.018 42.2186V45.8011H157.1V26.5107H171.66V30.0932H161.531V34.282H170.477V37.7543H161.531V42.2186H172.018Z"
                                                    fill="url(#paint10_linear_638:280)"/>
                                                <path
                                                    d="M201.663 39.6151H190.879L188.557 44.8552H186.427L195.276 25.2397H197.294L206.143 44.8552H203.986L201.663 39.6151ZM200.917 37.9337L196.271 27.3974L191.626 37.9337H200.917Z"
                                                    fill="url(#paint11_linear_638:280)"/>
                                                <path
                                                    d="M214.681 45.0233C213.243 45.0233 211.861 44.7898 210.534 44.3228C209.225 43.8557 208.211 43.2393 207.492 42.4733L208.294 40.8761C208.994 41.5859 209.925 42.1651 211.087 42.6134C212.248 43.0431 213.446 43.2579 214.681 43.2579C216.414 43.2579 217.714 42.9403 218.58 42.3052C219.446 41.6513 219.88 40.8107 219.88 39.7832C219.88 38.9986 219.64 38.3727 219.161 37.9057C218.7 37.4387 218.128 37.0837 217.446 36.8409C216.764 36.5793 215.815 36.2991 214.598 36.0002C213.142 35.6266 211.981 35.2716 211.114 34.9354C210.248 34.5804 209.501 34.048 208.875 33.3381C208.266 32.6282 207.962 31.6661 207.962 30.4518C207.962 29.4617 208.22 28.565 208.736 27.7617C209.252 26.9397 210.045 26.2859 211.114 25.8002C212.183 25.3145 213.511 25.0716 215.096 25.0716C216.202 25.0716 217.28 25.2304 218.331 25.548C219.4 25.8469 220.322 26.2672 221.096 26.809L220.405 28.4623C219.594 27.9205 218.728 27.5189 217.806 27.2573C216.884 26.9771 215.981 26.837 215.096 26.837C213.4 26.837 212.119 27.1733 211.253 27.8458C210.405 28.4996 209.981 29.3496 209.981 30.3958C209.981 31.1804 210.211 31.8156 210.672 32.3013C211.151 32.7683 211.741 33.1326 212.442 33.3942C213.16 33.637 214.119 33.9079 215.317 34.2068C216.737 34.5617 217.88 34.9167 218.746 35.2716C219.631 35.6079 220.377 36.131 220.986 36.8409C221.594 37.5321 221.898 38.4755 221.898 39.6711C221.898 40.6612 221.631 41.5673 221.096 42.3892C220.58 43.1925 219.778 43.8371 218.691 44.3228C217.603 44.7898 216.267 45.0233 214.681 45.0233Z"
                                                    fill="url(#paint12_linear_638:280)"/>
                                                <path
                                                    d="M231.693 45.0233C230.255 45.0233 228.873 44.7898 227.545 44.3228C226.237 43.8557 225.223 43.2393 224.504 42.4733L225.306 40.8761C226.006 41.5859 226.937 42.1651 228.098 42.6134C229.26 43.0431 230.458 43.2579 231.693 43.2579C233.426 43.2579 234.726 42.9403 235.592 42.3052C236.458 41.6513 236.892 40.8107 236.892 39.7832C236.892 38.9986 236.652 38.3727 236.173 37.9057C235.712 37.4387 235.14 37.0837 234.458 36.8409C233.776 36.5793 232.827 36.2991 231.61 36.0002C230.154 35.6266 228.993 35.2716 228.126 34.9354C227.26 34.5804 226.513 34.048 225.886 33.3381C225.278 32.6282 224.974 31.6661 224.974 30.4518C224.974 29.4617 225.232 28.565 225.748 27.7617C226.264 26.9397 227.057 26.2859 228.126 25.8002C229.195 25.3145 230.523 25.0716 232.108 25.0716C233.214 25.0716 234.292 25.2304 235.343 25.548C236.412 25.8469 237.334 26.2672 238.108 26.809L237.417 28.4623C236.606 27.9205 235.739 27.5189 234.818 27.2573C233.896 26.9771 232.993 26.837 232.108 26.837C230.412 26.837 229.131 27.1733 228.264 27.8458C227.416 28.4996 226.992 29.3496 226.992 30.3958C226.992 31.1804 227.223 31.8156 227.684 32.3013C228.163 32.7683 228.753 33.1326 229.453 33.3942C230.172 33.637 231.131 33.9079 232.329 34.2068C233.749 34.5617 234.891 34.9167 235.758 35.2716C236.643 35.6079 237.389 36.131 237.998 36.8409C238.606 37.5321 238.91 38.4755 238.91 39.6711C238.91 40.6612 238.643 41.5673 238.108 42.3892C237.592 43.1925 236.79 43.8371 235.703 44.3228C234.615 44.7898 233.278 45.0233 231.693 45.0233Z"
                                                    fill="url(#paint13_linear_638:280)"/>
                                                <path d="M243.368 25.2397H245.415V44.8552H243.368V25.2397Z"
                                                      fill="url(#paint14_linear_638:280)"/>
                                                <path
                                                    d="M257.049 45.0233C255.611 45.0233 254.229 44.7898 252.901 44.3228C251.592 43.8557 250.579 43.2393 249.86 42.4733L250.662 40.8761C251.362 41.5859 252.293 42.1651 253.454 42.6134C254.616 43.0431 255.814 43.2579 257.049 43.2579C258.782 43.2579 260.081 42.9403 260.948 42.3052C261.814 41.6513 262.247 40.8107 262.247 39.7832C262.247 38.9986 262.008 38.3727 261.528 37.9057C261.068 37.4387 260.496 37.0837 259.814 36.8409C259.132 36.5793 258.183 36.2991 256.966 36.0002C255.51 35.6266 254.348 35.2716 253.482 34.9354C252.616 34.5804 251.869 34.048 251.242 33.3381C250.634 32.6282 250.33 31.6661 250.33 30.4518C250.33 29.4617 250.588 28.565 251.104 27.7617C251.62 26.9397 252.413 26.2859 253.482 25.8002C254.551 25.3145 255.878 25.0716 257.464 25.0716C258.57 25.0716 259.648 25.2304 260.699 25.548C261.768 25.8469 262.69 26.2672 263.464 26.809L262.773 28.4623C261.962 27.9205 261.095 27.5189 260.173 27.2573C259.252 26.9771 258.349 26.837 257.464 26.837C255.768 26.837 254.487 27.1733 253.62 27.8458C252.772 28.4996 252.348 29.3496 252.348 30.3958C252.348 31.1804 252.579 31.8156 253.04 32.3013C253.519 32.7683 254.109 33.1326 254.809 33.3942C255.528 33.637 256.487 33.9079 257.685 34.2068C259.104 34.5617 260.247 34.9167 261.114 35.2716C261.998 35.6079 262.745 36.131 263.353 36.8409C263.962 37.5321 264.266 38.4755 264.266 39.6711C264.266 40.6612 263.999 41.5673 263.464 42.3892C262.948 43.1925 262.146 43.8371 261.058 44.3228C259.971 44.7898 258.634 45.0233 257.049 45.0233Z"
                                                    fill="url(#paint15_linear_638:280)"/>
                                                <path
                                                    d="M272.215 27.0331H265.413V25.2397H281.063V27.0331H274.261V44.8552H272.215V27.0331Z"
                                                    fill="url(#paint16_linear_638:280)"/>
                                                <path
                                                    d="M295.499 39.6151H284.715L282.392 44.8552H280.263L289.111 25.2397H291.13L299.978 44.8552H297.821L295.499 39.6151ZM294.752 37.9337L290.107 27.3974L285.461 37.9337H294.752Z"
                                                    fill="url(#paint17_linear_638:280)"/>
                                                <path
                                                    d="M319.356 25.2397V44.8552H317.669L305.226 28.9386V44.8552H303.18V25.2397H304.867L317.337 41.1563V25.2397H319.356Z"
                                                    fill="url(#paint18_linear_638:280)"/>
                                                <path
                                                    d="M329.434 27.0331H322.632V25.2397H338.282V27.0331H331.48V44.8552H329.434V27.0331Z"
                                                    fill="url(#paint19_linear_638:280)"/>
                                                <defs>
                                                    <linearGradient id="paint0_linear_638:280" x1="35.0947"
                                                                    y1="21.8157"
                                                                    x2="35.0947" y2="48.3739"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                    <linearGradient id="paint1_linear_638:280" x1="35.0947"
                                                                    y1="21.8157"
                                                                    x2="35.0947" y2="48.3739"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                    <linearGradient id="paint2_linear_638:280" x1="35.0947"
                                                                    y1="21.8157"
                                                                    x2="35.0947" y2="48.3739"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                    <linearGradient id="paint3_linear_638:280" x1="35.0947"
                                                                    y1="21.8157"
                                                                    x2="35.0947" y2="48.3739"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                    <linearGradient id="paint4_linear_638:280" x1="34.981"
                                                                    y1="32.4803"
                                                                    x2="34.981" y2="41.4808"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                    <linearGradient id="paint5_linear_638:280" x1="34.981"
                                                                    y1="32.4803"
                                                                    x2="34.981" y2="41.4808"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                    <linearGradient id="paint6_linear_638:280" x1="127.126"
                                                                    y1="26.1801"
                                                                    x2="127.126" y2="46.1318"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                    <linearGradient id="paint7_linear_638:280" x1="127.126"
                                                                    y1="26.1801"
                                                                    x2="127.126" y2="46.1318"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                    <linearGradient id="paint8_linear_638:280" x1="127.126"
                                                                    y1="26.1801"
                                                                    x2="127.126" y2="46.1318"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                    <linearGradient id="paint9_linear_638:280" x1="127.126"
                                                                    y1="26.1801"
                                                                    x2="127.126" y2="46.1318"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                    <linearGradient id="paint10_linear_638:280" x1="127.126"
                                                                    y1="26.1801"
                                                                    x2="127.126" y2="46.1318"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                    <linearGradient id="paint11_linear_638:280" x1="262.355"
                                                                    y1="25.0716"
                                                                    x2="262.355" y2="45.0233"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                    <linearGradient id="paint12_linear_638:280" x1="262.355"
                                                                    y1="25.0716"
                                                                    x2="262.355" y2="45.0233"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                    <linearGradient id="paint13_linear_638:280" x1="262.355"
                                                                    y1="25.0716"
                                                                    x2="262.355" y2="45.0233"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                    <linearGradient id="paint14_linear_638:280" x1="262.355"
                                                                    y1="25.0716"
                                                                    x2="262.355" y2="45.0233"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                    <linearGradient id="paint15_linear_638:280" x1="262.355"
                                                                    y1="25.0716"
                                                                    x2="262.355" y2="45.0233"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                    <linearGradient id="paint16_linear_638:280" x1="262.355"
                                                                    y1="25.0716"
                                                                    x2="262.355" y2="45.0233"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                    <linearGradient id="paint17_linear_638:280" x1="262.355"
                                                                    y1="25.0716"
                                                                    x2="262.355" y2="45.0233"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                    <linearGradient id="paint18_linear_638:280" x1="262.355"
                                                                    y1="25.0716"
                                                                    x2="262.355" y2="45.0233"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                    <linearGradient id="paint19_linear_638:280" x1="262.355"
                                                                    y1="25.0716"
                                                                    x2="262.355" y2="45.0233"
                                                                    gradientUnits="userSpaceOnUse">
                                                        <stop stop-color="#013E7A"/>
                                                        <stop offset="1" stop-color="#0E022A"/>
                                                    </linearGradient>
                                                </defs>
                                            </svg>
                                        </div>
                                    </Col>
                                    <Col sm={6}>
                                        <Container>
                                            <div className='searchBar'>
                                                <Row>
                                                    <Col sm={1}>
                                                        <div className='mic'>
                                                            <svg preserveAspectRatio="none" width="22" height="34"
                                                                 viewBox="0 0 22 34"
                                                                 fill="none"
                                                                 xmlns="http://www.w3.org/2000/svg">
                                                                <path
                                                                    d="M4.25 6.375C4.25 4.68425 4.92165 3.06274 6.11719 1.86719C7.31274 0.67165 8.93425 0 10.625 0C12.3158 0 13.9373 0.67165 15.1328 1.86719C16.3284 3.06274 17 4.68425 17 6.375V17C17 18.6908 16.3284 20.3123 15.1328 21.5078C13.9373 22.7034 12.3158 23.375 10.625 23.375C8.93425 23.375 7.31274 22.7034 6.11719 21.5078C4.92165 20.3123 4.25 18.6908 4.25 17V6.375Z"
                                                                    fill="url(#paint0_linear_638:277)"></path>
                                                                <path
                                                                    d="M1.0625 13.8125C1.34429 13.8125 1.61454 13.9244 1.8138 14.1237C2.01306 14.323 2.125 14.5932 2.125 14.875V17C2.125 19.2543 3.02053 21.4163 4.61459 23.0104C6.20865 24.6045 8.37066 25.5 10.625 25.5C12.8793 25.5 15.0413 24.6045 16.6354 23.0104C18.2295 21.4163 19.125 19.2543 19.125 17V14.875C19.125 14.5932 19.2369 14.323 19.4362 14.1237C19.6355 13.9244 19.9057 13.8125 20.1875 13.8125C20.4693 13.8125 20.7395 13.9244 20.9388 14.1237C21.1381 14.323 21.25 14.5932 21.25 14.875V17C21.25 19.634 20.2717 22.1741 18.5048 24.1276C16.7378 26.081 14.3083 27.3085 11.6875 27.5719V31.875H18.0625C18.3443 31.875 18.6145 31.9869 18.8138 32.1862C19.0131 32.3855 19.125 32.6557 19.125 32.9375C19.125 33.2193 19.0131 33.4895 18.8138 33.6888C18.6145 33.8881 18.3443 34 18.0625 34H3.1875C2.90571 34 2.63546 33.8881 2.4362 33.6888C2.23694 33.4895 2.125 33.2193 2.125 32.9375C2.125 32.6557 2.23694 32.3855 2.4362 32.1862C2.63546 31.9869 2.90571 31.875 3.1875 31.875H9.5625V27.5719C6.9417 27.3085 4.51217 26.081 2.74524 24.1276C0.978317 22.1741 -3.30898e-05 19.634 8.39388e-10 17V14.875C8.39388e-10 14.5932 0.111942 14.323 0.311199 14.1237C0.510457 13.9244 0.780707 13.8125 1.0625 13.8125Z"
                                                                    fill="url(#paint1_linear_638:277)"></path>
                                                                <defs>
                                                                    <linearGradient id="paint0_linear_638:277"
                                                                                    x1="10.625"
                                                                                    y1="0"
                                                                                    x2="10.625"
                                                                                    y2="23.375"
                                                                                    gradientUnits="userSpaceOnUse">
                                                                        <stop stop-color="#013E7A"></stop>
                                                                        <stop offset="1"
                                                                              stop-color="#0E022A"></stop>
                                                                    </linearGradient>
                                                                    <linearGradient id="paint1_linear_638:277"
                                                                                    x1="10.625"
                                                                                    y1="13.8125"
                                                                                    x2="10.625"
                                                                                    y2="34"
                                                                                    gradientUnits="userSpaceOnUse">
                                                                        <stop stop-color="#013E7A"></stop>
                                                                        <stop offset="1"
                                                                              stop-color="#0E022A"></stop>
                                                                    </linearGradient>
                                                                </defs>
                                                            </svg>
                                                        </div>
                                                    </Col>
                                                    <Col sm={1}>
                                                        <form className='input' onSubmit={handleSubmit}>
                                                            <input autoComplete="off" name="name"
                                                                   placeholder='Speak or Type Command Here'
                                                                   onChange={handleChange}/>
                                                        </form>
                                                    </Col>
                                                </Row>
                                            </div>
                                        </Container>
                                    </Col>

                                </Row>
                            </Container>
                        </div>
                    </div>
                    <header className="App-header">

                        <div className='loading'>{submitting ?
                            <div>
                                {/*<h3>LOADING</h3>*/}
                                <img src={spinner} alt="logo"/>
                            </div>
                            : ''}</div>
                        <div className='table'>{!submitting ?
                            getTable.status === 200 && !getError ?
                                <JsonToTable json={getTable.data.message}/>
                                :
                                getError ?
                                        'Unable to process query. Please try again.'
                                    :
                                    ''
                            :
                            ''}</div>
                    </header>
                </div>

                <div id='right' className={rightOpen}>
                    <div className='icon'
                         onClick={toggleSidebar}>
                        &equiv;
                    </div>
                    <div className='bordered'>
                        <div className={`sidebar ${rightOpen}`}>
                            <div className='header'>
                                <h2 className='titleRight'>
                                    Commands
                                </h2>
                            </div>
                            <div className='leftContent'>
                                <div className='header'>
                                    <h3 className='item'>
                                        Analytical Commands
                                    </h3>
                                </div>
                                <div className='header'>
                                    <p className='item'>
                                        Count
                                    </p>
                                </div>
                                <div className='header'>
                                    <p className='item'>
                                        Average
                                    </p>
                                </div>
                                <div className='header'>
                                    <p className='item'>
                                        Cross
                                    </p>
                                </div>
                                <div className='header'>
                                    <p className='item'>
                                        Compare
                                    </p>
                                </div>
                                <div className='header'>
                                    <h3 className='item'>
                                        Selection Commands
                                    </h3>
                                </div>
                                <div className='header'>
                                    <p className='item'>
                                        Breakout By
                                    </p>
                                </div>
                                <div className='header'>
                                    <p className='item'>
                                        Where / Filter
                                    </p>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}

export default App;